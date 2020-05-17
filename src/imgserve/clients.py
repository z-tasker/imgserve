from __future__ import annotations
import argparse
import os
import socket
from pathlib import Path

import boto3
from elasticsearch import Elasticsearch

from .elasticsearch import check_elasticsearch


def get_elasticsearch_args(
    parser: Optional[argparse.ArgumentParser] = None,
) -> argparse.ArgumentParser:

    if parser is None:
        parser = argparse.ArgumentParser()

    elasticsearch_parser = parser.add_argument_group("elasticsearch")

    elasticsearch_parser.add_argument(
        "--elasticsearch-client-fqdn",
        type=str,
        default=os.getenv("ES_CLIENT_FQDN", None),
        required=True,
        help="Elasticsearch address",
    )
    elasticsearch_parser.add_argument(
        "--elasticsearch-client-port",
        type=int,
        default=os.getenv("ES_CLIENT_PORT"),
        help="Elasticsearch port",
    )
    elasticsearch_parser.add_argument(
        "--elasticsearch-username",
        type=str,
        default=os.getenv("ES_USERNAME", None),
        required=True,
        help="Elasticsearch username",
    )
    elasticsearch_parser.add_argument(
        "--elasticsearch-password",
        type=str,
        default=os.getenv("ES_PASSWORD", None),
        required=True,
        help="Elasticsearch password",
    )
    elasticsearch_parser.add_argument(
        "--elasticsearch-ca-certs",
        type=Path,
        default=os.getenv("ES_CA_CERTS", None),
        required=False,
        help="Path to custom Elasticsearch CA. If Elasticsearch is behind a well used CA, this is not required. If Elasticsearch is behind self-signed certs, it is.",
    )

    return parser


def get_s3_args(
    parser: Optional[argparse.ArgumentParser] = None,
) -> argparse.ArgumentParser:

    if parser is None:
        parser = argparse.ArgumentParser()

    s3_parser = parser.add_argument_group("s3")

    s3_parser.add_argument(
        "--s3-region-name",
        type=str,
        default=os.getenv("AWS_REGION_NAME", None),
        help="S3 region",
    )
    s3_parser.add_argument(
        "--s3-endpoint-url",
        default=os.getenv("AWS_ENDPOINT", None),
        help="S3 endpoint URL (only required for non-AWS hosted",
    )
    s3_parser.add_argument(
        "--s3-access-key-id",
        type=str,
        default=os.getenv("AWS_ACCESS_KEY_ID", None),
        required=True,
    )
    s3_parser.add_argument(
        "--s3-secret-access-key",
        type=str,
        default=os.getenv("AWS_SECRET_ACCESS_KEY", None),
        required=True,
    )

    return parser


def get_experiment_args(
    parser: Optional[argparse.ArgumentParser] = None,
) -> argparse.ArgumentParser:

    if parser is None:
        parser = argparse.ArgumentParser()

    experiment_parser = parser.add_argument_group("experiment")

    mode = experiment_parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--dimensions",
        nargs="+",
        help="Analysis mode: provide fields to split images by, a folder of images will be created for each combination of values across each field",
    )
    mode.add_argument(
        "--run-trial",
        action="store_true",
        help="Trial mode: run the experiment queries to the (optionally) configured trial id",
    )
    mode.add_argument(
        "--share-ip-address",
        action="store_true",
        help="Share IP address information of this host to Elasticsearch, to facilitate analysis of global search results",
    )

    experiment_parser.add_argument(
        "--trial-ids",
        required=False,
        nargs="+",
        help="trial ids to gather images from",
    )
    experiment_parser.add_argument(
        "--trial-hostname",
        default=socket.gethostname(),
        help="hostname to use in metadata for images gathered by imgserve",
    )
    experiment_parser.add_argument(
        "--max-images",
        default=100,
        help="if --run-trial is set, number of images to collect",
    )
    experiment_parser.add_argument(
        "--experiment-name",
        required=True,
        help="Common name for the experiment this analysis supports",
    )
    experiment_parser.add_argument(
        "--no-local-data",
        action="store_true",
        help="Clear image data gathered for each search after each search, useful for very large, long-term trial runs.",
    )
    experiment_parser.add_argument(
        "--local-data-store",
        type=Path,
        required=True,
        help="Path to directory to store data locally",
    )
    experiment_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Take no action, but show what would happen",
    )
    experiment_parser.add_argument(
        "--s3-bucket", required=True, help="bucket where img data is stored in S3",
    )
    experiment_parser.add_argument(
        "--force-remote-pull",
        default=False,
        help="Pull images from S3 even if they are already on disk",
    )
    experiment_parser.add_argument(
        "--no-prompt",
        dest="prompt",
        action="store_false",
        help="don't prompt before potentially destructive actions",
    )
    experiment_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="overwrite existing assets: colorgram images in S3 and documents in Elasticsearch",
    )

    return parser


def get_imgserve_args(
    parser: Optional[argparse.ArgumentParser] = None,
) -> argparse.ArgumentParser:

    if parser is None:
        parser = argparse.ArgumentParser()

    imgserve_parser = parser.add_argument_group("imgserve")

    imgserve_parser.add_argument(
        "--remote-url",
        default="https://compsyn.fourtheye.xyz",
        help="url to use for source of experiment data, set to 'localhost:8080' to use local instance of the imgserve app",
    )
    imgserve_parser.add_argument(
        "--remote-username",
        required=False,
        help="username to use for authentication against remote imgserve",
    )
    imgserve_parser.add_argument(
        "--remote-password",
        required=False,
        help="username to use for authentication against remote imgserve",
    )
    imgserve_parser.add_argument(
        "--batch-slice",
        type=str,
        required=False,
        help="optionally slice experiment in multiple pieces for distributed running",
    )
    return parser


def get_clients(args: argparse.Namespace) -> Tuple[Elasticsearch, botocore.clients.s3]:
    """ Prepare clients required for processing """
    if args.elasticsearch_ca_certs is not None:
        assert (
            args.elasticsearch_ca_certs.is_file()
        ), f"{args.elasticsearch_ca_certs} not found!"

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
    check_elasticsearch(
        elasticsearch_client,
        args.elasticsearch_client_fqdn,
        args.elasticsearch_client_port,
    )

    s3_client = boto3.session.Session().client(
        "s3",
        region_name=args.s3_region_name,
        endpoint_url=args.s3_endpoint_url,
        aws_access_key_id=args.s3_access_key_id,
        aws_secret_access_key=args.s3_secret_access_key,
    )
    return elasticsearch_client, s3_client
