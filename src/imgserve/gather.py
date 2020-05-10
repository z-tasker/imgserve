from __future__ import annotations
import argparse
import os
import json
import shutil
from collections import defaultdict
from pathlib import Path
from tqdm import tqdm
from urllib.parse import urlparse

from .logger import simple_logger

log = simple_logger("gather raw images")


def summarize_experiment(manifest: Dict[str, Any]) -> str:
    """
        construct experiment results summary 
    """

    heirarchy = dict()
    malformed = 0
    for doc in manifest:
        try:
            query_term = doc["query"]
            region = doc["region"]
            image_domain = urlparse(doc["image_url"]).hostname
            if query_term not in heirarchy:
                heirarchy[query_term] = defaultdict(int)
                heirarchy[query_term]["domains"] = defaultdict(int)
            heirarchy[query_term][region] += 1
            heirarchy[query_term]["domains"][image_domain] += 1
        except KeyError as e:
            malformed += 1
            continue

    log.warning(f"{malformed}/{len(manifest)} documents malformed!")

    for query_term in list(heirarchy.keys()):
        top_domains = defaultdict(int)
        image_domains = heirarchy[query_term]["domains"]
        for k in sorted(image_domains, key=image_domains.get, reverse=True):
            if len(top_domains) <= 5:
                top_domains[k] = image_domains[k]
            else:
                top_domains["other"] += image_domains[k]
        heirarchy[query_term]["domains"] = top_domains

    return json.dumps(heirarchy, indent=2, ensure_ascii=False)


def download_experiment_data(
    experiment_id: str, local_path: Path, dry_run: bool = False
) -> Path:
    """
        download data for experiment not already present on disk
    """
    experiment_data_path = local_path.joinpath(experiment_id)
    if not dry_run:
        experiment_data_path.mkdir(exist_ok=True, parents=True)
    client = get_s3_client()
    paginator = client.get_paginator("list_objects")
    manifests = list()
    total_images = 0
    total_bytes = 0
    to_download = list()
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=f"data/{experiment_id}"):
        for obj in page["Contents"]:
            object_path = Path(obj["Key"])
            if object_path.suffix == ".json":
                manifests.extend(
                    json.loads(
                        client.get_object(Bucket=BUCKET_NAME, Key=str(object_path))[
                            "Body"
                        ].read()
                    )
                )
            else:
                total_images += 1
                total_bytes += obj["Size"]
                if not dry_run:
                    to_download.append(object_path)

    already_downloaded = 0
    with tqdm(
        total=total_bytes, desc="Downloaded", unit="bytes", unit_scale=True
    ) as pbar:
        for object_path in to_download:
            local_image_path = experiment_data_path.joinpath(
                object_path.relative_to(f"data/{experiment_id}")
            )
            local_image_path.parent.mkdir(exist_ok=True, parents=True)
            if not local_image_path.is_file():
                obj = client.get_object(Bucket=BUCKET_NAME, Key=str(object_path))
                local_image_path.write_bytes(obj["Body"].read())
                pbar.update(obj["ContentLength"])
            else:
                already_downloaded += 1
                pbar.update(len(local_image_path.read_bytes()))

    log.info(f"{total_images} images found ({total_bytes/1000000:.1f} mb)")
    log.info(f"{already_downloaded} images were already on disk.")

    return experiment_data_path, manifests


def gather_manifest_docs(path: Path) -> Generator[Dict[str, Any], None, None]:
    for p in path.iterdir():
        if p.is_dir():
            yield from gather_manifest_docs(p)
        elif p.name == "manifest.json":
            yield from json.loads(p.read_text())


def gather_raw_images(
    experiment_id: str,
    local_data_path: Path,
    dry_run: bool = False,
    skip_download: bool = False,
) -> None:
    if not skip_download:
        log.info(f"gathering results for {experiment_id} from S3...")
        experiment_data_path, manifests = download_experiment_data(
            experiment_id, local_path=local_data_path, dry_run=dry_run
        )
    else:
        experiment_data_path = local_data_path.joinpath(experiment_id)
        manifests = [img_doc for img_doc in gather_manifest_docs(experiment_data_path)]

    experiment_data_path.joinpath("manifest.json").write_text(
        json.dumps(manifests, indent=2, ensure_ascii=False)
    )
    experiment_data_path.joinpath("summary.json").write_text(
        summarize_experiment(manifests)
    )
