from __future__ import annotations

import os
from pathlib import Path

import cv2
from PIL import Image


FACE_CLASSIFIER_XML = os.getenv("IMGSERVE_FACE_CLASSIFIER_XML", "haarcascade_frontalface_default.xml")
FACE_CLASSIFIER = cv2.CascadeClassifier(FACE_CLASSIFIER_XML)


def facechop(image: Path, output_dir: Path, face_classifier: cv2.CascadeClassifier = FACE_CLASSIFIER) -> Generator[Path, None, None]:  
    img = cv2.imread(image)

    minisize = (img.shape[1],img.shape[0])
    miniframe = cv2.resize(img, minisize)

    faces = face_classifier.detectMultiScale(miniframe)

    for f in faces:
        x, y, w, h = [ v for v in f ]
        cv2.rectangle(img, (x,y), (x+w,y+h), (255,255,255))

        sub_face = img[y:y+h, x:x+w]
        face_file_name = "face_" + str(y) + "_" + image 
        output_path = output_dir.joinpath(face_file_name) 
        cv2.imwrite(output_path, sub_face)
        yield output_path
