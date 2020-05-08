#!/usr/bin/env python3
from __future__ import annotations
import argparse
import logging
from pathlib import Path

import boto3
from elasticsearch import Elasticsearch

from args import get_elasticsearch_args, get_s3_args
from assemble import assemble
from vectors import get_vectors

logging.basicConfig(
    format=f"%(asctime)s %(levelname)s:%(message)s", datefmt="%Y-%m-%dT%H:%M:%S"
)


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
        "--s3-bucket", default="qload", help="bucket where img data is stored in S3",
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

    return parser.parse_args()


def get_clients(args: argparse.Namespace) -> Tuple[Elasticsearch, botocore.clients.s3]:
    """ Prepare clients required for processing """
    elasticsearch_client = Elasticsearch(
        hosts=[
            {
                "host": args.elasticsearch_client_fqdn,
                "port": args.elasticsearch_client_port,
            }
        ],
        http_auth=(args.elasticsearch_username, args.elasticsearch_password),
        use_ssl=True,
        verify_certs=True,
        ca_certs=args.elasticsearch_ca_certs,
    )
    s3_client = boto3.session.Session().client(
        "s3",
        region_name=args.s3_region_name,
        endpoint_url=args.s3_endpoint_url,
        aws_access_key_id=args.s3_access_key_id,
        aws_secret_access_key=args.s3_secret_access_key,
    )
    return elasticsearch_client, s3_client


def main(args: argparse.Namespace) -> None:
    """ image analysis from the point of archive """

    elasticsearch_client, s3_client = get_clients(args)

    # assemble "downloads" folder from data collected in experiment_ids according to the dimensions configured
    downloads: Path = assemble(
        elasticsearch_client=elasticsearch_client,
        s3_client=s3_client,
        bucket_name=args.s3_bucket,
        experiment_ids=args.experiment_ids,
        dimensions=args.dimensions,
        local_data_store=args.local_data_store,
        dry_run=args.dry_run,
        force_remote_pull=args.force_remote_pull,
        prompt=args.prompt,
    )

    if args.dry_run:
        logging.info("--dry-run passed, cannot continue past here")
        return

    for vector, metadata in get_vectors(downloads):
        # save colorgram in S3
        # associate metadata with vector, index
        pass


if __name__ == "__main__":
    main(parse_args())
