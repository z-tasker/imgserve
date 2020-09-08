from __future__ import annotations
import pytest

from pathlib import Path

from imgserve.faces import facechop

@pytest.mark.unit
def test_facechop() -> None:
    successes = list()
    failures = list()
    for test_img in Path(__file__).parent.joinpath("faces").iterdir():
        if test_img.suffix != ".jpg":
            continue
        test_img = test_img.name
        face_count = 0
        for face_img in facechop(
            image=Path(__file__).parent.joinpath("faces").joinpath(test_img), 
            output_dir=Path(__file__).parent.joinpath("faces/extracted")
        ):
            face_count += 1
        try: 
            min_faces, max_faces = [int(el) for el in test_img.split("-")[0].split("to")]
        except ValueError as exc:
            min_faces = int(test_img.split("-")[0])
            max_faces = min_faces
        if min_faces <= face_count and face_count <= max_faces:
            successes.append(f"{test_img}.jpg unexpected number of faces! [{min_faces}, {max_faces}] expected, {face_count} gathered")
        else:
            failures.append(f"{test_img}.jpg unexpected number of faces! [{min_faces}, {max_faces}] expected, {face_count} gathered")

    print("successes:", len(successes))
    print("failures:", len(failures))
    assert len(failures) == 0, "\n".join(failures)
