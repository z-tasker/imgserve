#!/usr/bin/env python3
from __future__ import annotations
import argparse
import io
import os
import json
import socket
from pathlib import Path

import requests
from PIL import Image

from imgserve import get_experiment_colorgrams_path, get_experiment_csv_path, STATIC
from imgserve.api import ImgServe, Experiment
from imgserve.assemble import assemble_downloads
from imgserve.args import (
    get_elasticsearch_args,
    get_experiment_args,
    get_imgserve_args,
    get_s3_args,
)
from imgserve.clients import get_clients
from imgserve.elasticsearch import get_response_value, index_to_elasticsearch, COLORGRAMS_INDEX_PATTERN
from imgserve.logger import simple_logger
from imgserve.s3 import s3_put_image
from imgserve.trial import run_trial
from imgserve.vectors import get_vectors


BUCKET_NAME = "imgserve"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    get_elasticsearch_args(parser)
    get_s3_args(parser)
    get_experiment_args(parser)
    get_imgserve_args(parser)

    return parser.parse_args()


class AmbiguousTrialIDError(Exception):
    pass


class MissingRequiredArgError(Exception):
    pass


def get_ipinfo() -> Generator[Dict[str, Any]]:
    ipinfo = requests.get("https://ipinfo.io").json()

    yield ipinfo


def default_trial_id(
    experiment_name: str,
    hostname: str = os.getenv("IMGSERVE_HOSTNAME", socket.gethostname()),
) -> str:
    return "-".join([hostname, experiment_name])


def main(args: argparse.Namespace) -> None:
    """ image gathering trial and analysis of arbitrary trials"""

    log = simple_logger("imgserve.experiment")

    log.info(f"starting {args.experiment_name}...")

    elasticsearch_client, s3_client = get_clients(args)

    experiment = Experiment(
        bucket_name=args.s3_bucket,
        elasticsearch_client=elasticsearch_client,
        local_data_store=args.local_data_store,
        name=args.experiment_name,
        s3_client=s3_client,
        dry_run=args.dry_run,
        debug=args.debug
    )

    imgserve = ImgServe(
        remote_url=args.remote_url,
        username=args.remote_username,
        password=args.remote_password,
    )

    if args.share_ip_address:
        index_to_elasticsearch(
            elasticsearch_client,
            index="hosts",
            docs=get_ipinfo(),
            identity_fields=None,  # will always write a new document on this call
            overwrite=False,
        )
        log.info(f"shared ip address to Elasticsearch, thanks!")
        return

    if args.trial_ids is None:
        args.trial_ids = [default_trial_id(args.experiment_name)]

    if args.run_trial:
        if len(args.trial_ids) > 1:
            raise AmbiguousTrialIDError(
                "when running a trial, please pass a maximum of one trial ID to --trial-ids, this is the id the new results will be associated with. Pass no --trial-ids for a sane default"
            )
        else:
            trial_id = args.trial_ids[0]

        log.info(
            f"launching an image gathering trial, associating results with the identifier '{trial_id}'"
        )

        if args.prompt:
            if input("sound good? (y/n) ").lower() not in ["y", "yes"]:
                log.info("does not sound good, exiting.")
                return

        run_trial(
            elasticsearch_client=elasticsearch_client,
            experiment_name=args.experiment_name,
            local_data_store=args.local_data_store,
            s3_access_key_id=args.s3_access_key_id,
            s3_bucket_name=args.s3_bucket,
            s3_client=s3_client,
            s3_endpoint_url=args.s3_endpoint_url,
            s3_region_name=args.s3_region_name,
            s3_secret_access_key=args.s3_secret_access_key,
            trial_config=imgserve.get_experiment(args.experiment_name),
            trial_hostname=args.trial_hostname,
            trial_id=trial_id,
            batch_slice=args.batch_slice,
            dry_run=args.dry_run,
            max_images=args.max_images,
            no_local_data=args.no_local_data,
            run_user_browser_scrape=args.run_user_browser_scrape,
            skip_already_searched=args.skip_already_searched,
            skip_vectors=args.skip_vectors,
        )

        log.info(f"image gathering completed")
        return

    if args.trial_ids is None:
        raise Exception("Must pass trial ids explicitly until TODO completed")
        # TODO: write this method
        trial_ids = get_trial_ids(experiment_name=experiment_name)
        # Check if trial_id in args.dimensions, warn results will mix if not

    if args.dimensions is not None:
        log.info(
            f"assembling 'downloads' folder from data, splitting images by {args.dimensions}..."
        )
        downloads: Path = assemble_downloads(
            elasticsearch_client=elasticsearch_client,
            s3_client=s3_client,
            bucket_name=args.s3_bucket,
            trial_ids=args.trial_ids,
            experiment_name=args.experiment_name,
            dimensions=args.dimensions,
            local_data_store=args.local_data_store,
            dry_run=args.dry_run,
            force_remote_pull=args.force_remote_pull,
            prompt=args.prompt,
        )

        if args.dry_run:
            log.info("--dry-run passed, cannot continue past here")
            return

        # create compsyn.vectors.Vector objects out of each folder, and also store metadata for Elasticsearch
        log.info(f"generating vectors from {downloads}...")
        colorgram_documents = list()
        colorgrams_path = get_experiment_colorgrams_path(
            local_data_store=args.local_data_store,
            app_static_path=STATIC,
            name=args.experiment_name,
        )
        for vector, metadata in get_vectors(downloads):
            # store colorgram images in S3
            s3_put_image(
                s3_client=s3_client,
                image=vector.colorgram,
                bucket=args.s3_bucket,
                object_path=Path(args.experiment_name).joinpath(metadata["s3_key"]),
                overwrite=args.overwrite,
            )
            # save colorgram locally, regardless of overwrite
            vector.colorgram.save(
                colorgrams_path.joinpath(vector.word).with_suffix(".png")
            )
            # queue colorgram metadata for indexing to Elasticsearch
            metadata.update(experiment_name=args.experiment_name)
            colorgram_documents.append(metadata)

        log.info(f"{len(colorgram_documents)} colorgrams persisted to S3, indexing...")

        index_to_elasticsearch(
            elasticsearch_client=elasticsearch_client,
            index=COLORGRAMS_INDEX_PATTERN,
            docs=colorgram_documents,
            identity_fields=["experiment_name", "downloads", "s3_key"],
            overwrite=args.overwrite,
        )

        log.info(
            f"finished experiment analysis. Local colorgrams from this experiment: {colorgrams_path}"
        )

    if args.delete:
        experiment.delete()

    if args.pull:
        experiment.pull()

    if args.from_archive_path is not None:
        manifest_path = args.from_archive_path.joinpath("manifest.json")
        if not manifest_path.is_file():
            raise FileNotFoundError(f"{manifest_path} not found, cannot index")

        manifests = json.loads(manifest_path.read_text())

        for manifest in manifests:
            manifest["experiment_name"] = args.experiment_name
            manifest["trial_id"] = f"archive-{args.experiment_name}"
            manifest["trial_timestamp"] = manifest["ran_at"]
            rel_path = (
                Path(manifest["hostname"])
                .joinpath(manifest["query"])
                .joinpath(manifest["trial_timestamp"])
                .joinpath(f"images")
                .joinpath(manifest["image_id"])
                .with_suffix(".jpg")
            )

            image_path = args.from_archive_path.joinpath(rel_path)
            image = Image.open(image_path).convert("RGB")
            with open(image_path, "w") as f:
                image.resize((300, 300), Image.ANTIALIAS).save(
                    f, "JPEG", optimize=True, quality=85
                )
            s3_put_image(
                s3_client=s3_client,
                image=image_path,
                bucket=args.s3_bucket,
                object_path=Path("data")
                .joinpath(manifest["trial_id"])
                .joinpath(rel_path),
                overwrite=args.overwrite,
            )
            if not image_path.is_file():
                raise FileNotFoundError(
                    f"while processing archive {args.from_archive_path}, could not find local source image {image_path}"
                )

        index_to_elasticsearch(
            elasticsearch_client,
            index="raw-images",
            docs=manifests,
            identity_fields=["experiment_name", "trial_id", "trial_timestamp"],
            overwrite=args.overwrite,
        )

    if args.get is not None:
        for doc, img_path in experiment.get(args.get):
            del doc["_source"]["downloads"]
            print(json.dumps(doc, indent=2))
            print(img_path)
            image = Image.open(img_path)
            image.show()
            input("continue...")
            image.close()



        



if __name__ == "__main__":
    main(parse_args())
