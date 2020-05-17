from __future__ import annotations
import copy
import json
import shlex
import shutil
import subprocess
from datetime import datetime
from pathlib import Path

from retry import retry

from .elasticsearch import index_to_elasticsearch, RAW_IMAGES_INDEX_PATTERN
from .errors import UnimplementedError
from .logger import simple_logger
from .utils import get_batch_slice

QUERY_RUNNER_IMAGE = "mgraskertheband/qloader:4.0.0"


@retry(tries=10, backoff=5)
def run_search(docker_run_command: str) -> None:
    with open("qloader.log", "a") as f:
        subprocess.run(
            shlex.split(docker_run_command), stdin=None, stdout=f, stderr=f, check=True,
        )


def run_trial(
    elasticsearch_client: Elasticsearch,
    s3_access_key_id: str,
    s3_secret_access_key: str,
    s3_endpoint_url: str,
    s3_region_name: str,
    s3_bucket_name: str,
    trial_id: str,
    trial_config: Dict[str, Any],
    trial_hostname: str,
    experiment_name: str,
    local_data_store: Path,
    no_local_data: bool = False,
    max_images: int = 100,
    dry_run: bool = False,
    endpoint: str = "google-images",
    run_user_browser_scrape: bool = False,
    strict_config: bool = False,
    verbose: bool = False,
    batch_slice: Optional[str] = None,
) -> None:
    """
        Light wrapper around github.com/mgrasker/qloader containerized search gatherer.
        Results are uploaded to S3 in the container, this method will handle indexing the raw image metadata to elasticsearch.
    """
    log = simple_logger("run_trial")
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
    else:
        trial_slice = trial_config_items
        log.info(f"running slice {batch_slice} of {experiment_name}")

    # for each search_term in csv, launch docker query
    # TODO: optional "user browser" query
    for search_term, csv_metadata in trial_slice:
        regions = csv_metadata.pop("regions")

        if strict_config and trial_hostname not in regions:
            log.info(
                f"{trial_hostname} does not appear in the configured regions for the query {search_term}, skipping"
            )
            continue
        if dry_run:
            log.info(f"would run search {search_term}, but --dry-run is set")
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
            log.info(f"running image gathering container for query: {search_term}")
            docker_run_command = f'docker run \
                --user 1000:1000 \
                --shm-size=2g \
                -v {local_data_store}:/tmp/imgserve \
                --env QLOADER_BROWSER=Chrome \
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
            run_search(docker_run_command)

        trial_run_manifest = (
            local_data_store.joinpath(trial_id)
            .joinpath(trial_hostname)
            .joinpath(trial_timestamp)
            .joinpath("manifest.json")
        )
        if not trial_run_manifest.is_file():
            raise FileNotFoundError(
                f"The trial run should have created a manifest file at {trial_run_manifest}, but it did not!"
            )
        index_to_elasticsearch(
            elasticsearch_client=elasticsearch_client,
            index=RAW_IMAGES_INDEX_PATTERN,
            docs=json.loads(trial_run_manifest.read_text()),
            identity_fields=["trial_id", "trial_hostname", "ran_at"],
        )
        if no_local_data:
            shutil.rmtree(
                local_data_store.joinpath(trial_id)
                .joinpath(trial_hostname)
                .joinpath(trial_timestamp)
            )
