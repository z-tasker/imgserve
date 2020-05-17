from __future__ import annotations

import boto3
from elasticsearch import Elasticsearch

from .elasticsearch import check_elasticsearch


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
