#!/usr/bin/env python3
from __future__ import annotations
import csv
import os
import logging
from collections import defaultdict
from pathlib import Path

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, StreamingResponse
from starlette.routing import Route, Mount, WebSocketRoute
from starlette.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates
from starlette.websockets import WebSocket

from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware

from imgserve import get_experiment_csv_path, STATIC, LOCAL_DATA_STORE

middleware = [
    Middleware(
        CORSMiddleware,
        allow_origins=["compsyn.fourtheye.xyz", "compsyn.fourtheye.xyz:443"],
        allow_headers=["*"],
        allow_methods=["*"],
    )
]

app = Starlette(middleware=middleware)
app.mount("/static", StaticFiles(directory=STATIC), name="static")

templates = Jinja2Templates(directory="templates")

log = logging.getLogger()


async def open_experiment_csv(csv_path: Path) -> Dict[str, Dict[str, Any]]:
    region_column = "region"
    query_terms_column = "search_term"
    # marshal around query terms
    unique_queries = dict()
    with open(csv_path, encoding="utf-8-sig", newline="") as csvf:
        for query in csv.DictReader(csvf, dialect="excel"):
            regions = query.pop(region_column).split(" ")
            search_term = query.pop(query_terms_column)
            for region in regions:
                if search_term in unique_queries:
                    unique_queries[search_term]["regions"].append(region.lower())
                else:
                    unique_queries[search_term] = {"regions": [region.lower()]}
                    unique_queries[search_term].update(**query)
    return unique_queries


@app.route("/")
async def search(request: Request):
    template = "home.html"

    experiments = [p.name for p in Path("static/img/colorgrams").glob("*")]
    context = {"request": request, "experiments": experiments}
    return templates.TemplateResponse(template, context)


@app.route("/search")
async def search(request: Request):
    template = "search.html"
    context = {
        "request": request,
        "experiments": [p.name for p in Path("static/img/colorgrams").glob("*")],
        "x_values": ["query"],
        "y_values": ["region"],
        "z_values": ["time", "domain", "eng_ref"],
    }

    return templates.TemplateResponse(template, context)


@app.route("/tesselation")
async def tesselation(request: Request):
    template = "pages.html"

    experiment = request.query_params["experiment"]
    x = request.query_params["x"]
    y = request.query_params["y"]
    z = request.query_params["z"]

    all_colorgrams = [
        p.stem for p in Path(f"static/img/colorgrams/{experiment}").glob("*")
    ]

    pages = defaultdict(set)
    x_values = set()
    y_values = set()

    for colorgram_slug in all_colorgrams:
        tags = colorgram_slug.split("|")
        for tag in tags:
            tag_key, tag_value = tag.split("=")
            if tag_key == x:
                x_values.add(tag_value)
            elif tag_key == y:
                y_values.add(tag_value)
            elif tag_key == z:
                pages[tag_key].add(tag_value)

    def get_colorgram_with(x_value: str, y_value: str, z_value: str) -> str:
        for colorgram_slug in all_colorgrams:
            if (
                f"{x}={x_value}" in colorgram_slug
                and f"{y}={y_value}" in colorgram_slug
                and f"{z}={z_value}" in colorgram_slug
            ):
                return colorgram_slug

    colorgram_pages = dict()
    for page_key in pages.keys():
        page = defaultdict(dict)
        for x_value in x_values:
            for y_value in y_values:
                page[x_value][y_value] = get_colorgram_with(
                    x_value=x_value, y_value=y_value, z_value=page_key
                )
        colorgram_pages[page_key] = page

    context = {
        "experiment": experiment,
        "request": request,
        "x_values": x_values,
        "colorgram_pages": colorgram_pages,
    }

    return templates.TemplateResponse(template, context)


@app.route("/langip_grids_{langip_name}")
async def generated(request: Request):
    template = "langip.html"

    colorgrams = defaultdict(dict)
    experiment = Path(request["path"]).name
    all_colorgrams = sorted(
        [
            f.stem
            for f in Path(__file__)
            .parent.joinpath(f"static/img/colorgrams/{experiment}")
            .iterdir()
        ]
    )
    eng_refs = defaultdict(list)
    for cg in all_colorgrams:
        dimensions = {dim.split("=")[0]: dim.split("=")[1] for dim in cg.split("|")}
        eng_refs[dimensions["eng_ref"]].append(dimensions["query"])
        colorgrams[dimensions["query"]][cg] = ", ".join(
            [f"{value}" for attr, value in sorted(dimensions.items())]
        )

    queries = list(colorgrams.keys())

    # manually order

    eng_ref_sorted = dict()
    for eng_ref in eng_refs:
        manually_ordered = defaultdict(dict)
        for region in ["fra1", "ams3", "nyc1", "blr1", "sgp1"]:
            queries = list(eng_refs[eng_ref])
            eng_center = list()
            for query in queries:
                if query == eng_ref:
                    continue
                eng_center.append(query)
            eng_center.insert(2, eng_ref)
            for query in eng_center:
                for col, val in colorgrams[query].items():
                    if region in val:
                        manually_ordered[query][col] = val
        eng_ref_sorted[eng_ref] = manually_ordered

    context = {
        "experiment": experiment,
        "request": request,
        "queries": queries,
        "colorgrams": eng_ref_sorted,
    }
    return templates.TemplateResponse(template, context)


@app.route("/experiments/{experiment_name}")
async def experiment_csv(request: Request) -> Response:

    experiment_name = request.path_params["experiment_name"]

    try:
        response = await open_experiment_csv(
            get_experiment_csv_path(
                name=experiment_name, local_data_store=LOCAL_DATA_STORE,
            )
        )
    except FileNotFoundError as e:
        log.error(f"{experiment_name}: {e}")
        response = {
            "error": 404,
            "missing": experiment_name,
            "inventory": [
                csv.stem
                for csv in STATIC.joinpath("csv/experiments/").glob("*")
                if csv.suffix == ".csv"
            ],
        }
    except KeyError as e:
        log.error(f"{experiment_name}: {e}")
        response = {
            "error": 422,
            "invalid": experiment_name,
            "message": f"{experiment_name}.csv was found, but is missing a required column: {e}",
        }

    return JSONResponse(response)


@app.route("/{experiment}")
async def generated(request: Request):
    template = "index.html"

    colorgrams = defaultdict(dict)
    experiment = Path(request["path"]).name
    all_colorgrams = sorted(
        [
            f.stem
            for f in Path(__file__)
            .parent.joinpath(f"static/img/colorgrams/{experiment}")
            .iterdir()
        ]
    )
    for cg in all_colorgrams:
        dimensions = {dim.split("=")[0]: dim.split("=")[1] for dim in cg.split("|")}
        colorgrams[dimensions["query"]][cg] = ", ".join(
            [f"{value}" for attr, value in sorted(dimensions.items())]
        )

    queries = list(colorgrams.keys())

    context = {
        "experiment": experiment,
        "request": request,
        "queries": queries,
        "colorgrams": colorgrams,
    }
    return templates.TemplateResponse(template, context)


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8080)
