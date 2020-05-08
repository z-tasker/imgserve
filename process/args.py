from __future__ import annotations
import argparse


def get_elasticsearch_args(
    parser: Optional[argparse.ArgumentParser] = None,
) -> argparse.ArgumentParser:

    if parser is None:
        parser = argparse.ArgumentParser()

    elasticsearch_parser = parser.add_argument_group("elasticsearch")

    elasticsearch_parser.add_argument(
        "--elasticsearch-client-fqdn",
        type=str,
        required=True,
        help="Elasticsearch address",
    )
    elasticsearch_parser.add_argument(
        "--elasticsearch-client-port", type=int, default=9200, help="Elasticsearch port"
    )
    elasticsearch_parser.add_argument(
        "--elasticsearch-username",
        type=str,
        required=True,
        help="Elasticsearch username",
    )
    elasticsearch_parser.add_argument(
        "--elasticsearch-password",
        type=str,
        required=True,
        help="Elasticsearch password",
    )
    elasticsearch_parser.add_argument(
        "--elasticsearch-ca-certs",
        type=Path,
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
        "--s3-region-name", type=str, default="fra1", help="S3 region"
    )
    s3_parser.add_argument(
        "--s3-endpoint-url",
        type=str,
        default="https://fra1.digitaloceanspaces.com",
        help="S3 endpoint URL (only required for non-AWS hosted",
    )
    s3_parser.add_argument("--s3-access-key-id", type=str, required=True)
    s3_parser.add_argument("--s3-secret-access-key", type=str, required=True)

    return parser
