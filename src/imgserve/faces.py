from __future__ import annotations

import os
from pathlib import Path

import cv2

from .logger import simple_logger


FACE_CLASSIFIER_XML = os.getenv("IMGSERVE_FACE_CLASSIFIER_XML", "haarcascade_frontalface_alt.xml")
assert Path(FACE_CLASSIFIER_XML).is_file(), f"{FACE_CLASSIFIER_XML} is not a file!"

FACE_CLASSIFIER = cv2.CascadeClassifier(FACE_CLASSIFIER_XML)


class NotAnImageError(Exception):
    pass


def scale_image(img: cv2.Image, target_height: int = 500) -> cv2.Image:
    """ fit height box """
    height, width = img.shape[:2]
    height_ratio = target_height / height
    return cv2.resize(img, (int(height_ratio*width), int(height_ratio*height)), interpolation = cv2.INTER_CUBIC)


def facechop(image: Path, output_dir: Path, face_classifier: cv2.CascadeClassifier = FACE_CLASSIFIER, cv2_cascade_min_neighbors: int = 5) -> Generator[Path, None, None]:

    if not image.is_file():
        raise FileNotFoundError(image)

    log = simple_logger("imgserve.facechop")

    img = cv2.imread(str(image))

    if img is None:
        raise NotAnImageError(f"file exists, but is not an image.")
    minisize = (img.shape[1], img.shape[0])
    miniframe = cv2.resize(img, minisize)

    faces = face_classifier.detectMultiScale(miniframe, minNeighbors=cv2_cascade_min_neighbors)

    count = 0
    padding_pct = 0.2
    for f in faces:
        x, y, w, h = [ v for v in f ]
        padding = int(h * padding_pct)
        cv2.rectangle(img, (x,y), (x+w+padding,y+h+padding), (255,255,255))
        sub_face = img[y:y+h, x:x+w]

        output_dir.mkdir(exist_ok=True, parents=True)
        cropped_face_image: Path = output_dir.joinpath(image.stem + f"-{count}").with_suffix(".jpg")
        log.debug(f"writing {cropped_face_image}")
        cv2.imwrite(str(cropped_face_image), scale_image(sub_face))

        yield cropped_face_image
        count += 1
