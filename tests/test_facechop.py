from __future__ import annotations
import pytest

from pathlib import Path

from imgserve.faces import facechop

@pytest.mark.unit
def test_facechop() -> None:
    for test_img in ["0-faces.jpg", "1-face.jpg", "3-faces.jpg"]:
        face_count = 0
        for face_img in facechop(
            image=Path(__file__).parent.joinpath("faces").joinpath(test_img), 
            output_dir=Path(__file__).parent.joinpath("faces/extracted")
        ):
            face_count += 1
        expected_faces = int(test_img.split("-")[0])
        assert face_count == expected_faces, f"unexpected number of faces! {expected_faces} expected, {face_count} gathered"
            
