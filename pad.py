#!/usr/bin/env python3
from __future__ import annotations
from collections import defaultdict
from pathlib import Path

colorgrams = defaultdict(dict)
experiments = Path("static/img/colorgrams")
missing = Path("static/img/missing.png")

def pad_experiment(directory: Path) -> None:
    colorgrams = [ f.stem for f in directory.iterdir() ]
    all_queries = set()
    all_regions = set()
    for colorgram in colorgrams:
        dimensions = { dim.split("=")[0]: dim.split("=")[1] for dim in colorgram.split("|") }
        all_queries.add(dimensions["query"])
        all_regions.add(dimensions["region"])

    region_first = True if str(list(directory.iterdir())[0].name).startswith("region") else False
    for query in all_queries:
        for region in all_regions:
            if region_first:
                colorgram_path = directory.joinpath(f"region={region}|query={query}.png")
            else:
                colorgram_path = directory.joinpath(f"query={query}|region={region}.png")
            if not colorgram_path.is_file():
                print(f"write missing to {colorgram_path}")
                colorgram_path.write_bytes(missing.read_bytes())
    




for directory in ( p for p in experiments.iterdir() if p.is_dir() ):
    pad_experiment(directory)
