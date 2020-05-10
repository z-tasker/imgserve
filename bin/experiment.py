#!/usr/bin/env python3
from __future__ import annotations
import argparse
import io
import json
from pathlib import Path

from imgserve import get_experiment_colorgrams_path, get_experiment_csv_path, STATIC
from imgserve.api import ImgServe
from imgserve.assemble import assemble_downloads
from imgserve.clients import get_elasticsearch_args, get_experiment_args, get_imgserve_args, get_s3_args, get_clients
from imgserve.elasticsearch import index_to_elasticsearch
from imgserve.logger import simple_logger
from imgserve.s3 import s3_put_image
from imgserve.trial import run_trial

from vectors import get_vectors

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


def main(args: argparse.Namespace) -> None:
    """ image gathering trial and analysis of arbitrary trials"""

    log = simple_logger(f"experiment")

    log.info(f"starting {args.experiment_name}...")

    elasticsearch_client, s3_client = get_clients(args)

    imgserve = ImgServe(
        remote_url=args.remote_url,
        username=args.remote_username,
        password=args.remote_password,
    )

    if args.run_trial:
        if len(args.trial_ids) > 1:
            raise AmbiguousTrialIDError("when running a trial, please pass only one trial ID to --trial-ids, this is the id the new results will be associated with")

        log.info(f"launching an image gathering trial, associating results with the identifier '{args.trial_ids[0]}'")

        if args.prompt:
            if input("sound good? (y/n) ").lower() not in ["y", "yes"]:
                log.info("does not sound good, exiting.")
                return 

        run_trial(
            elasticsearch_client=elasticsearch_client,
            max_images=args.max_images,
            s3_access_key_id=args.s3_access_key_id,
            s3_secret_access_key=args.s3_secret_access_key,
            s3_endpoint_url=args.s3_endpoint_url,
            s3_region_name=args.s3_region_name,
            s3_bucket_name=args.s3_bucket,
            trial_id=args.trial_ids[0],
            trial_config=imgserve.get_experiment(args.experiment_name),
            trial_hostname=args.trial_hostname,
            experiment_name=args.experiment_name,
            local_data_store=args.local_data_store,
            dry_run=args.dry_run,
        )

        log.info(f"image gathering completed, to analyze results from this trial identifier drop the --run-trial flag")
        return 

    if args.dimensions is None:
        raise MissingRequiredArgError(f"You must pass --dimensions when running image analysis, refer to README for a description of what these do")

    log.info(f"assembling 'downloads' folder from data, splitting images by {args.dimensions}...")
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
            get_experiment_colorgrams_path(
                local_data_store=args.local_data_store,
                app_static_path=STATIC,
                name=args.experiment_name,
            )
            .joinpath(vector.word)
            .with_suffix(".png")
        )
        # queue colorgram metadata for indexing to Elasticsearch
        metadata.update(experiment_name=args.experiment_name)
        colorgram_documents.append(metadata)

    log.info(f"{len(colorgram_documents)} colorgrams persisted to S3, indexing...")

    index_to_elasticsearch(
        elasticsearch_client=elasticsearch_client,
        index="colorgrams",
        docs=colorgram_documents,
        identity_fields=["experiment_name", "downloads", "s3_key"],
        overwrite=args.overwrite,
    )


if __name__ == "__main__":
    main(parse_args())
