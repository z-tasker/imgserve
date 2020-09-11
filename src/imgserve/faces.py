from __future__ import annotations

import os
from pathlib import Path

import cv2

from .logger import simple_logger


FACE_CLASSIFIER_XML = os.getenv("IMGSERVE_FACE_CLASSIFIER_XML", "haarcascade_frontalface_alt.xml")
assert Path(FACE_CLASSIFIER_XML).is_file(), f"{FACE_CLASSIFIER_XML} is not a file!"

FACE_CLASSIFIER = cv2.CascadeClassifier(FACE_CLASSIFIER_XML)



def facechop(image: Path, output_dir: Path, face_classifier: cv2.CascadeClassifier = FACE_CLASSIFIER) -> Generator[Path, None, None]:
    log = simple_logger("imgserve.facechop")
    img = cv2.imread(str(image))

    minisize = (img.shape[1], img.shape[0])
    miniframe = cv2.resize(img, minisize)

    faces = face_classifier.detectMultiScale(miniframe, minNeighbors=5)

    count = 0
    for f in faces:
        x, y, w, h = [ v for v in f ]
        cv2.rectangle(img, (x,y), (x+w,y+h), (255,255,255))
        sub_face = img[y:y+h, x:x+w]

        output_dir.mkdir(exist_ok=True, parents=True)
        cropped_face_image: Path = output_dir.joinpath(image.stem + f"-{count}").with_suffix(".jpg")
        log.debug(f"writing {cropped_face_image}")
        cv2.imwrite(str(cropped_face_image), sub_face)

        yield cropped_face_image
        count += 1
