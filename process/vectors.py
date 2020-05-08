#!/usr/bin/env python3
from __future__ import annotations
import hashlib
from pathlib import Path

from compsyn.vectors import Vector


def tags_to_hash(tags: List[str]) -> str:

    m = hashlib.sha256()
    for tag in sorted(tags):
        m.update(tag.encode("utf-8"))

    return m.hexdigest()

def get_vectors(
    downloads_path: Path,
) -> Generator[Tuple[Vector, Dict[str, Any]], None, None]:
    for folder in downloads_path.iterdir():
        tags = str(folder.stem).split("|")
        metadata = {key: value for key, value in (tag.split("=") for tag in tags)}
        metadata.update({"s3_key": tags_to_hash(tags)})
        yield Vector(folder.name, downloads_path), metadata
