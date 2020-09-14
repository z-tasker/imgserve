from __future__ import annotations

import hashlib
from pathlib import Path

import imagehash
import pytest
from imgcat import imgcat
from PIL import Image

# from imgserve.utils import get_unique_image

def hashes_to_id(hashes: Tuple[Array]) -> str:
    morse_code = ""
    for image_hash in hashes:
        for char in image_hash.hash.flatten():
            if char:
                morse_code += "."
            else:
                morse_code += "_"
    print(morse_code)
    return hashlib.md5(morse_code.encode("utf-8")).hexdigest()


def get_unique_images(images_path: Path, show_matches: bool = False) -> Tuple[int, Dict[Tuple[imagehash.ImageHash, imagehash.ImageHash], List[Path]]]:
    image_hashes: Dict[Tuple[str, str], List[Path]] = dict()
    img_count = 0
    for test_img in images_path.iterdir():
        img_count += 1
        img = Image.open(test_img).resize((300,300))
        color_hash = imagehash.colorhash(img)
        average_hash = imagehash.average_hash(img)

        
        if len(image_hashes) == 0:
            image_hashes[(color_hash, average_hash)] = [test_img]

        for existing_hashes, existing_paths in image_hashes.items():
            existing_color_hash, existing_average_hash = existing_hashes
            color_diff = existing_color_hash - color_hash
            average_diff = existing_average_hash - average_hash
            if average_diff == 0:
                if color_diff == 0:
                    if show_matches:
                        print(f"similar images (average_diff={average_diff} color_diff={color_diff})")
                        imgcat(Image.open(existing_paths[0]))
                        imgcat(img)
                        print()
                    image_hashes[existing_hashes].append(test_img)
            
        recorded_paths = list()
        for paths in image_hashes.values():
            recorded_paths.extend(paths)
        if len(set(recorded_paths)) < img_count:
            image_hashes[(color_hash, average_hash)] = [test_img]

    return img_count, image_hashes


def test_duplicate_image_hashing() -> None:
    img_count, image_hashes = get_unique_images(Path(__file__).parent.joinpath("duplicates"))
    print("unique images:", len(image_hashes))
    print("total images: ", img_count)
    assert len(image_hashes) == 178
    assert img_count == 587

    img_count, image_hashes = get_unique_images(Path(__file__).parent.joinpath("similar"))
    print("unique images:", len(image_hashes))
    print("total images: ", img_count)
    assert len(image_hashes) == 90
    assert img_count == 97

