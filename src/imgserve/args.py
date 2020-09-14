from __future__ import annotations
import argparse
import os
import socket
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
        "--s3-bucket", required=True, help="bucket where img data is stored in S3",
    )
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


def get_mturk_args(
    parser: Optional[argparse.ArgumentParser] = None,
) -> argparse.ArgumentParser:

    if parser is None:
        parser = argparse.ArgumentParser()

    mturk_parser = parser.add_argument_group("mturk")

    mturk_parser.add_argument(
        "--mturk-s3-bucket-name",
        type=str,
        required=False
    )
    mturk_parser.add_argument(
        "--mturk-aws-region",
        type=str,
        required=False
    )
    mturk_parser.add_argument(
        "--mturk-access-key-id",
        type=str,
        default=os.getenv("MTURK_ACCESS_KEY_ID"),
        required=False,
    )
    mturk_parser.add_argument(
        "--mturk-secret-access-key",
        type=str,
        default=os.getenv("MTURK_SECRET_ACCESS_KEY"),
        required=False,
    )
    mturk_parser.add_argument(
        "--mturk-cropped-face-images-hit-type-id",
        type=str,
        required=False
    )
    mturk_parser.add_argument(
        "--mturk-cropped-face-images-hit-layout-id",
        type=str,
        required=False
    )
    mturk_parser.add_argument(
        "--mturk-raw-images-hit-type-id",
        type=str,
        required=False
    )
    mturk_parser.add_argument(
        "--mturk-raw-images-hit-layout-id",
        type=str,
        required=False
    )
    mturk_parser.add_argument(
        "--mturk-colorgrams-hit-type-id",
        type=str,
        required=False
    )
    mturk_parser.add_argument(
        "--mturk-colorgrams-hit-layout-id",
        type=str,
        required=False
    )
    mturk_parser.add_argument(
        "--mturk-in-realtime",
        action="store_true",
        help="Create Mturk HITs at search time, default behaviour only creates mturk_hit_documents in Elasticsearch"
    )
    mturk_parser.add_argument(
        "--create-mturk-cropped-face-images-hits",
        action="store_false",
        dest="skip_mturk_cropped_face_images",
    )
    mturk_parser.add_argument(
        "--create-mturk-raw-images-hits",
        action="store_false",
        dest="skip_mturk_raw_images",
    )
    mturk_parser.add_argument(
        "--create-mturk-colorgrams-hits",
        action="store_false",
        dest="skip_mturk_colorgrams",
    )

    return parser

def get_experiment_args(
    parser: Optional[argparse.ArgumentParser] = None,
) -> argparse.ArgumentParser:

    if parser is None:
        parser = argparse.ArgumentParser()

    experiment_parser = parser.add_argument_group("experiment")

    experiment_parser.add_argument(
        "--experiment-name",
        required=True,
        help="Common name for the experiment this analysis supports.",
    )
    experiment_parser.add_argument(
        "--local-data-store",
        type=Path,
        required=True,
        help="Path to directory to store data locally",
    )

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
    mode.add_argument(
        "--delete",
        action="store_true",
        help="Delete data associated with this experiment name",
    )
    mode.add_argument(
        "--get",
        type=str,
        help="Get and display colorgram for the provided query term from this experiment name",
    )
    mode.add_argument(
        "--pull",
        action="store_true",
        help="Pull colorgrams associated with this experiment name",
    )
    mode.add_argument(
        "--from-archive-path",
        type=Path,
        help="Load raw-images from an expanded archive",
    )
    mode.add_argument(
        "--label",
        action="store_true",
        help="Label a folder of colorgrams where filename == s3_key according to --dimensions, must include args --unlabeled-data-path and --label-write-path",
    )
    mode.add_argument(
        "--export-vectors-to",
        type=Path,
        help="export colorgram documents as a JSON list",
    )
    mode.add_argument(
        "--get-unique-images",
        action="store_true",
        help="Gather unique images by using image_url as the source of cross-trial image identity",
    )
    mode.add_argument(
        "--create-mturk-hits",
        action="store_true",
        help="Create Mturk HIT objects from the experiment"
    )

    experiment_parser.add_argument(
        "--fresh-url-download",
        action="store_true",
        help="Where possible, download the image from the source URL for highest-fi image version",
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
        "--top-wikipedia-articles",
        type=int,
        help="create the experiment.csv dynamically from the configured number of wikipedia articles",
    )
    experiment_parser.add_argument(
        "--max-images",
        default=100,
        help="if --run-trial is set, number of images to collect",
    )
    experiment_parser.add_argument(
        "--skip-vectors",
        action="store_true",
        help="Don't create vectors from each search as they complete.",
    )
    experiment_parser.add_argument(
        "--no-local-data",
        action="store_true",
        help="Clear image data gathered for each search after each search, useful for very large, long-term trial runs.",
    )
    experiment_parser.add_argument(
        "--skip-already-searched",
        action="store_true",
        help="don't search terms this host has already run for the trial_id",
    )
    experiment_parser.add_argument(
        "--run-user-browser-scrape",
        action="store_true",
        help="use the python environment's selenium driver, instead of the safer qloader container",
    )
    experiment_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Take no action, but show what would happen",
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
    experiment_parser.add_argument(
        "--debug",
        action="store_true",
        help="provide additional output to help debug queries, etc",
    )
    experiment_parser.add_argument(
        "--unlabeled-data-path",
        type=Path,
        help="folder of colorgrams with filename as s3_key",
    )
    experiment_parser.add_argument(
        "--label-write-path", type=Path, help="folder to write labeled colorgrams to"
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
        default="https://comp-syn.ialcloud.xyz",
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
    imgserve_parser.add_argument(
        "--no-compress",
        action="store_true",
        help="Do not compress images before mirroring them to S3. Default behaviour is to compress to 300x300 (stretch to fit)."
    )
    imgserve_parser.add_argument(
        "--extract-faces",
        dest="skip_face_detection",
        action="store_false",
        help="Extract faces from raw images and store in their own index in Elasticsearch",
    )
    imgserve_parser.add_argument(
        "--cv2-cascade-min-neighbors",
        type=int,
        default=5,
        help="minNeighbors hyperparameter for cv2 haarcascade based face classification"
    )
    return parser
