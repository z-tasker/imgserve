#!/usr/bin/env python3
from __future__ import annotations
from pathlib import Path

from compsyn.vectors import Vector


def get_vectors(
    downloads_path: Path,
) -> Generator[Tuple[Vector, Dict[str, Any]], None, None]:
    for folder in downloads_path.iterdir():
        tags = str(folder).split("|")
        metadata = {key: value for key, value in (tag.split("=") for tag in tags)}
        yield Vector(folder.name, downloads_path), metadata
