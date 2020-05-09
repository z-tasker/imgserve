#!/usr/bin/env python3
from __future__ import annotations
import argparse
import io
import json
from pathlib import Path

from imgserve import get_experiment_colorgrams_path, get_experiment_csv_path, STATIC
from imgserve.assemble import assemble_downloads
from imgserve.clients import get_elasticsearch_args, get_s3_args, get_clients
from imgserve.elasticsearch import index_to_elasticsearch
from imgserve.logger import simple_logger
from imgserve.s3 import s3_put_image

from vectors import get_vectors

BUCKET_NAME = "imgserve"
COLORGRAMS_INDEX_TEMPLATE = json.loads(
    Path(__file__).parents[1].joinpath("db/colorgrams.template.json").read_text()
)
assert (
    len(COLORGRAMS_INDEX_TEMPLATE) > 0
), f"the index template 'db/colorgrams.template.json' must exist"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    get_elasticsearch_args(parser)
    get_s3_args(parser)
    parser.add_argument(
        "--experiment-ids",
        required=True,
        nargs="+",
        help="experiment ids to gather images from",
    )
    parser.add_argument(
        "--local-data-store",
        type=Path,
        required=True,
        help="Path to directory to store data locally",
    )
    parser.add_argument(
        "--downloads-directory",
        type=Path,
        default="imgserve/process",
        help="Path to assemble images in 'downloads' folder",
    )
    parser.add_argument(
        "--dimensions",
        required=True,
        nargs="+",
        help="fields to split images by, a folder of images will be created for each combination of values across each field",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Take no action, but show what would happen",
    )
    parser.add_argument(
        "--s3-bucket", required=True, help="bucket where img data is stored in S3",
    )
    parser.add_argument(
        "--force-remote-pull",
        default=False,
        help="Pull images from S3 even if they are already on disk",
    )
    parser.add_argument(
        "--no-prompt",
        dest="prompt",
        action="store_false",
        help="don't prompt before potentially destructive actions",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="overwrite existing assets: colorgram images in S3 and documents in Elasticsearch",
    )

    return parser.parse_args()


def main(args: argparse.Namespace) -> None:
    """ image analysis from the point of archive """

    log = simple_logger(f"colorgrams")

    log.info("starting image processing...")

    elasticsearch_client, s3_client = get_clients(args)

    # assemble "downloads" folder from data collected in experiment_ids according to the dimensions configured
    downloads: Path = assemble_downloads(
        elasticsearch_client=elasticsearch_client,
        s3_client=s3_client,
        bucket_name=args.s3_bucket,
        experiment_ids=args.experiment_ids,
        dimensions=args.dimensions,
        local_data_store=args.local_data_store,
        downloads_directory=args.downloads_directory,
        dry_run=args.dry_run,
        force_remote_pull=args.force_remote_pull,
        prompt=args.prompt,
    )

    meta_id = "-and-".join(args.experiment_ids)

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
            object_path=Path(meta_id).joinpath(metadata["s3_key"]),
            overwrite=args.overwrite,
        )
        # save colorgram locally, regardless of overwrite
        vector.colorgram.save(
            get_experiment_colorgrams_path(
                local_data_store=args.local_data_store,
                app_static_path=STATIC,
                name=meta_id,
            )
            .joinpath(vector.word)
            .with_suffix(".png")
        )
        # queue colorgram metadata for indexing to Elasticsearch
        colorgram_documents.append(metadata)

    log.info(f"{len(colorgram_documents)} colorgrams persisted to S3, indexing...")

    elasticsearch_client.indices.put_template(
        name="colorgrams", body=COLORGRAMS_INDEX_TEMPLATE
    )
    index_to_elasticsearch(
        elasticsearch_client=elasticsearch_client,
        index="colorgrams",
        docs=colorgram_documents,
        identity_fields=["experiment_id", "downloads", "s3_key"],
        overwrite=args.overwrite,
    )


if __name__ == "__main__":
    main(parse_args())
