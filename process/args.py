from __future__ import annotations
import argparse
import os
from pathlib import Path


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
        default=os.getenv("SPACES_REGION_NAME", None),
        help="S3 region",
    )
    s3_parser.add_argument(
        "--s3-endpoint-url",
        type=str,
        default=os.getenv("SPACES_ENDPOINT", None),
        help="S3 endpoint URL (only required for non-AWS hosted",
    )
    s3_parser.add_argument(
        "--s3-access-key-id",
        type=str,
        default=os.getenv("SPACES_ACCESS_KEY_ID", None),
        required=True,
    )
    s3_parser.add_argument(
        "--s3-secret-access-key",
        type=str,
        default=os.getenv("SPACES_SECRET_ACCESS_KEY", None),
        required=True,
    )

    return parser
