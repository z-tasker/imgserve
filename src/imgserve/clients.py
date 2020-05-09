from __future__ import annotations
import argparse
import os
from pathlib import Path

import boto3
from elasticsearch import Elasticsearch


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
        default=os.getenv("ES_CLIENT_PORT", 9200),
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
        required=True,
        help="Path to Elasticsearch CA",
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


def get_clients(args: argparse.Namespace) -> Tuple[Elasticsearch, botocore.clients.s3]:
    """ Prepare clients required for processing """
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
    s3_client = boto3.session.Session().client(
        "s3",
        region_name=args.s3_region_name,
        endpoint_url=args.s3_endpoint_url,
        aws_access_key_id=args.s3_access_key_id,
        aws_secret_access_key=args.s3_secret_access_key,
    )
    return elasticsearch_client, s3_client
