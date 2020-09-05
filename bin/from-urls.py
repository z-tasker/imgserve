#!/usr/bin/env python3
from __future__ import annotations

import csv
import tempfile
import json
from io import BytesIO
from pathlib import Path

import requests
from PIL import Image

from imgserve.vectors import get_vectors


def iterate_csv(
    path: Path, delimiter: str = ","
) -> Generator[Dict[str, Union[str, int, float]]]:
    with open(path) as tsvf:
        read_tsv = csv.reader(tsvf, delimiter=delimiter)

        header = None
        for row in read_tsv:
            if header is None:
                header = row
                continue
            yield {key: val for key, val in zip(header, row)}


def download_image(url: str, path: Path) -> Path:

    resp = requests.get(url)
    if resp.status_code == 200:
        if len(vector_metadata) % 100 == 0:
            print(len(vector_metadata), "vectors created")
        folder_name = "|".join([f"{key}={val}" for key, val in row.items()])
        path.parent.mkdir(exist_ok=True, parents=True)
        img = Image.open(BytesIO(resp.content))
        try:
            img.save(path)
        except OSError as exc:
            raise ImageDownloadError(f"Could not save image from {url}") from exc


def from_csv(
    csv_file: Path, output: Path, url_field: str, id_field: str, delimiter: str
) -> None:

    temp_dir = Path(tempfile.TemporaryDirectory().name)
    vector_metadata = list()
    for row in iterate_csv(path=csv_file, delimiter="\t"):
        # set up a transient "downloads" folder for each item, download the image, run compsyn, add the vector to the list

        url = row.pop("image_url")
        img_path = output.parent.joinpath("MSD-I-images").joinpath(
            row["msd_track_id"] + "-" + Path(url).name
        )
        download_image(url, img_path)

    print("images:", temp_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv-file", type=Path, help="Path to csv file with urls")
    parser.add_argument("--output", type=Path, help="Path to store images at")
    parser.add_argument("--url-field", type=str, help="Column name with urls")
    parser.add_argument("--id-field", type=str, help="Column to use for image identity")
    parser.add_argument("--delimiter", default=",", help="CSV delimiter")
    from_csv(
        csv_file=args.csv_file,  # Path("/Volumes/LACIE/compsyn/data/MSD-I_dataset.tsv")
        output=args.output,  # Path(__file__).parent.joinpath("MSD-I-vectors.json")
        url_field=args.url_field,  # "image_url"
        id_field=args.id_field,  # "msd_track_id"
        delimiter=args.delimiter,  # "\t"
    )

#        # run compsyn
#        try:
#            for vector, metadata in get_vectors(downloads_path=img_path.parent.parent):
#                del metadata["s3_key"]
#                del metadata["downloads"]
#                del metadata["jzazbz_dist_std"]
#                vector_metadata.append(metadata)
#        except IndexError:
#            print("index error!")
#            continue
#    else:
#        print(f"Could not gather image: {resp.status_code}")
#
# output.write_text(json.dumps(vector_metadata, indent=2))
