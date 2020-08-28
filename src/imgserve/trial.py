from __future__ import annotations
import copy
import json
import shlex
import shutil
import subprocess
from datetime import datetime
from pathlib import Path

from retry import retry

from .elasticsearch import (
    document_exists,
    index_to_elasticsearch,
    COLORGRAMS_INDEX_PATTERN,
    RAW_IMAGES_INDEX_PATTERN,
)
from .errors import UnimplementedError
from .logger import simple_logger
from .s3 import s3_put_image
from .utils import get_batch_slice
from .vectors import get_vectors

QUERY_RUNNER_IMAGE = "mgraskertheband/qloader:4.3.0"


@retry(tries=5, backoff=2, delay=1)
def run_search(docker_run_command: str, timeout: int) -> None:
    log = simple_logger("imgserve.subprocess")
    proc = subprocess.run(
        shlex.split(docker_run_command), capture_output=True, timeout=timeout
    )
    log.debug("stdout: " + proc.stdout.decode("utf-8"))
    log.debug("stderr: " + proc.stderr.decode("utf-8"))

    if proc.returncode != 0:
        raise subprocess.CalledProcessError(proc.returncode, docker_run_command)


def run_trial(
    elasticsearch_client: Elasticsearch,
    experiment_name: str,
    local_data_store: Path,
    s3_access_key_id: str,
    s3_bucket_name: str,
    s3_client: botocore.clients.s3,
    s3_endpoint_url: str,
    s3_region_name: str,
    s3_secret_access_key: str,
    trial_config: Dict[str, Any],
    trial_hostname: str,
    trial_id: str,
    batch_slice: Optional[str] = None,
    dry_run: bool = False,
    endpoint: str = "google-images",
    max_images: int = 100,
    no_local_data: bool = False,
    run_user_browser_scrape: bool = False,
    skip_already_searched: bool = False,
    skip_vectors: bool = False,
    query_timeout: int = 300
) -> None:
    """
        Light wrapper around github.com/mgrasker/qloader containerized search gatherer.
        Results are uploaded to S3 in the container, this method will handle indexing the raw image metadata to elasticsearch.
    """
    log = simple_logger("imgserve.run_trial")
    trial_timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    shared_metadata = {
        "trial_id": trial_id,
        "hostname": trial_hostname,
        "trial_timestamp": trial_timestamp,
        "experiment_name": experiment_name,
    }

    trial_config_items = list(trial_config.items())
    # optionally slice experiment into chunks and only run one
    if batch_slice is not None:
        trial_slice = get_batch_slice(items=trial_config_items, batch_slice=batch_slice)
        log.info(f"running slice {batch_slice} of {experiment_name}")
    else:
        trial_slice = trial_config_items

    # for each search_term in csv, launch docker query
    # TODO: optional "user browser" query
    for search_term, csv_metadata in trial_slice:

        if skip_already_searched and document_exists(
            elasticsearch_client=elasticsearch_client,
            doc={
                "hostname": trial_hostname,
                "query": search_term,
                "trial_id": trial_id,
            },
            index="raw-images",
            identity_fields=["hostname", "query", "trial_id"],
        ):
            log.info(f"already searched {search_term} from this host for {trial_id}")
            continue

        regions = csv_metadata.pop("regions")

        if dry_run:
            log.info(f"[DRY RUN] would run search {search_term}")
            continue

        image_document_shared = copy.deepcopy(shared_metadata)
        image_document_shared.update({"region": trial_hostname})
        image_document_shared.update(csv_metadata)
        search_metadata_log = local_data_store.joinpath(trial_id).joinpath(
            f".metadata-{trial_timestamp}.json"
        )
        search_metadata_log.parent.mkdir(exist_ok=True, parents=True)
        search_metadata_log.write_text(json.dumps(image_document_shared, indent=2))
        if run_user_browser_scrape:
            raise UnimplementedError()
        else:
            log.info(f"running {QUERY_RUNNER_IMAGE} for query: {search_term}")
            docker_run_command = f'docker run \
                --user 1000:1000 \
                --shm-size=2g \
                -v {local_data_store}:/tmp/imgserve \
                --env QLOADER_BROWSER=Firefox \
                --env S3_ACCESS_KEY_ID={s3_access_key_id} \
                --env S3_SECRET_ACCESS_KEY={s3_secret_access_key} \
                --env S3_ENDPOINT_URL={s3_endpoint_url} \
                --env S3_REGION_NAME={s3_region_name} \
                --env S3_BUCKET_NAME={s3_bucket_name} \
                {QUERY_RUNNER_IMAGE} \
                    --trial-id {trial_id} \
                    --hostname {trial_hostname} \
                    --ran-at {trial_timestamp} \
                    --endpoint {endpoint} \
                    --query-terms "{search_term}" \
                    --max-images {max_images} \
                    --output-path /tmp/imgserve/ \
                    --metadata-path /tmp/imgserve/{trial_id}/.metadata-{trial_timestamp}.json'
            try:
                run_search(docker_run_command, timeout=query_timeout)
            except subprocess.TimeoutExpired as e:
                log.error(f"Query for {search_term} took longer than {query_timeout}, skipping.")
                continue

        query_downloads = (
            local_data_store.joinpath(trial_id)
            .joinpath(trial_hostname)
            .joinpath(trial_timestamp)
        )
        trial_run_manifest = query_downloads.joinpath("manifest.json")
        (
            local_data_store.joinpath(trial_id)
            .joinpath(trial_hostname)
            .joinpath(trial_timestamp)
            .joinpath("manifest.json")
        )
        if not trial_run_manifest.is_file():
            from .pathtree import DisplayablePath
            paths = DisplayablePath.make_tree(local_data_store)

            raise FileNotFoundError(
                "\n".join(
                    [
                        f"The trial run should have created a manifest file at {trial_run_manifest}, but it did not!",
                        f"here's what was at {local_data_store}:",
                        "\n".join([path.displayable() for path in paths]),
                    ]
                )
            )
        index_to_elasticsearch(
            elasticsearch_client=elasticsearch_client,
            index=RAW_IMAGES_INDEX_PATTERN,
            docs=json.loads(trial_run_manifest.read_text()),
            identity_fields=["trial_id", "trial_hostname", "ran_at"],
        )
        if not skip_vectors:
            vector_stem = f"query={search_term}|hostname={trial_hostname}|trial_timestamp={trial_timestamp}"
            trial_downloads = query_downloads.joinpath("vector").joinpath(vector_stem)
            try:
                shutil.rmtree(
                    trial_downloads
                )  # clear existing downloads folders from previous runs
            except FileNotFoundError:
                pass
            trial_downloads.mkdir(parents=True)
            for downloaded_image in query_downloads.joinpath("images").glob("*.jpg"):
                trial_downloads.joinpath(downloaded_image.name).write_bytes(
                    downloaded_image.read_bytes()
                )
            documents = list()
            for vector, metadata in get_vectors(trial_downloads.parent):
                s3_put_image(
                    s3_client=s3_client,
                    image=vector.colorgram,
                    bucket=s3_bucket_name,
                    object_path=Path(experiment_name).joinpath(metadata["s3_key"]),
                    overwrite=True,
                )
                metadata.update(experiment_name=experiment_name)
                documents.append(metadata)
                if not no_local_data:
                    save_to = (
                        trial_downloads.parents[1]
                        .joinpath("colorgrams")
                        .joinpath(vector_stem)
                        .with_suffix(".png")
                    )
                    save_to.parent.mkdir(exist_ok=True, parents=True)
                    vector.colorgram.save(save_to)
            if len(documents) > 1:
                log.warning(f"multiple vectors created from a single search run")
            index_to_elasticsearch(
                elasticsearch_client=elasticsearch_client,
                index=COLORGRAMS_INDEX_PATTERN,
                docs=documents,
                identity_fields=["experiment_name", "downloads", "s3_key"],
                overwrite=False,
            )
            log.info(
                f"vector for '{search_term}' indexed and saved to s3"
                + (
                    f", and also here: {trial_downloads.with_suffix('.png')}"
                    if not no_local_data
                    else ""
                )
            )

        if no_local_data:
            shutil.rmtree(query_downloads)
            log.info(f"removed '{search_term}' data from local storage")
