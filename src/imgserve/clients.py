from __future__ import annotations

import boto3
from elasticsearch import Elasticsearch

from .elasticsearch import check_elasticsearch
from .errors import MissingArgumentsError
from .logger import simple_logger


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
        timeout=300,
        http_auth=(args.elasticsearch_username, args.elasticsearch_password),
        use_ssl=True,
        verify_certs=True,
        #        ca_certs=args.elasticsearch_ca_certs,
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


def get_mturk_client(args: argparse.Namespace) -> Optional[botocore.clients.mturk]:

    arg_issues = list()
    if args.skip_face_detection:
        if not args.skip_mturk_cropped_face_images:
            arg_issues.append("Cannot create cropped face HITs while skipping face detection")

    if args.mturk_s3_bucket_name is None:
        args.mturk_s3_bucket_name = args.s3_bucket


    if args.mturk_access_key_id is None and args.mturk_secret_access_key is None:
        args.mturk_access_key_id = args.s3_access_key_id
        args.mturk_secret_access_key = args.s3_secret_access_key

    if args.mturk_aws_region is None:
        args.mturk_aws_region = args.s3_region_name

    create_client = False
    for mturk_target in ["raw_images", "cropped_face_images", "colorgrams"]:
        if not getattr(args, f"skip_mturk_{mturk_target}"):
            create_client = True # only create client if one of the mturk flags is set
            missing = list()
            for required in [
                f"mturk_{mturk_target}_hit_type_id",
                f"mturk_{mturk_target}_hit_layout_id",
                f"mturk_access_key_id",
                f"mturk_secret_access_key"
            ]:
                if getattr(args, required) is None:
                    missing.append(required)

            if len(missing) > 0:
                arg_issues.append(
                    ",".join(missing) + f" are required arguments when --create-mturk-{mturk_target.replace('_', '-')}-hits is set"
                )

    if len(arg_issues) > 0:
        raise MissingArgumentsError("\nAND\n".join(arg_issues))

    if create_client:
        ret = boto3.client(
            "mturk",
            aws_access_key_id=args.mturk_access_key_id,
            aws_secret_access_key=args.mturk_secret_access_key,
            region_name=args.mturk_aws_region,
        )
    else:
        ret = None
    return ret
