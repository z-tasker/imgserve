#!/usr/bin/env python3
from pathlib import Path

from imgserve.faces import facechop, NotAnImageError

downloads_path = Path("/Volumes/LACIE/compsyn/data/unrest/original")

extracted_faces = 0
for query_dir in downloads_path.iterdir():
    query = query_dir.name
    for image_path in query_dir.iterdir():
        try:
            for face_image in facechop(
                image_path, downloads_path.parent.joinpath("faces").joinpath(query)
            ):
                extracted_faces += 1
        except NotAnImageError as exc:
            print(exc)

print(f"extracted {extracted_faces} faces")
