#!/usr/bin/env python3
from __future__ import annotations
import argparse
import base64
import copy
import csv
import os
import json
import logging
from collections import defaultdict
from pathlib import Path

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import (
    HTMLResponse,
    JSONResponse,
    StreamingResponse,
    RedirectResponse,
)
from starlette.routing import Route, Mount, WebSocketRoute
from starlette.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates
from starlette.websockets import WebSocket

from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware

from imgserve import get_experiment_csv_path, STATIC, LOCAL_DATA_STORE
from imgserve.api import Experiment
from imgserve.args import get_elasticsearch_args, get_s3_args
from imgserve.clients import get_clients

from vectors import get_experiments

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


def get_raw_data_link(experiment: str) -> Optional[str]:
    link = Path(f"static/dropbox-links/{experiment}")
    if link.is_file():
        return link.read_text()
    else:
        return None


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


async def respond_with_404(request: Request, message: str):
    response = templates.TemplateResponse(
        "404.html", {"request": request, "message": message,},
    )
    return response


@app.route("/")
async def home(request: Request):
    template = "home.html"

    experiments = get_experiments(ELASTICSEARCH_CLIENT)
    results = [p.name for p in Path("static/img/colorgrams").glob("*")]

    context = {"request": request, "experiments": experiments, "results": results}
    return templates.TemplateResponse(template, context)


@app.route("/archive")
async def archive(request: Request):
    if "experiment" in request.query_params:
        experiment = request.query_params["experiment"]
        dl_link = get_raw_data_link(experiment)
        if dl_link is None:
            response = await respond_with_404(
                request=request, message=f"No download link available for {experiment}"
            )
        else:
            response = RedirectResponse(url=dl_link)
    else:
        template = "archive.html"
        experiments = get_experiments(ELASTICSEARH_CLIENT)
        context = {"request": request, "experiments": experiments}
        response = templates.TemplateResponse(template, context)

    return response


@app.route("/search")
async def search(request: Request):
    template = "search.html"

    experiments = get_experiments(ELASTICSEARCH_CLIENT)

    context = {"request": request, "experiments": experiments}
    return templates.TemplateResponse(template, context)


@app.route("/sketch")
async def sketch(request: Request):

    default_experiment = None
    try:
        default_experiment = request.query_params["default_experiment"]
    except KeyError:
        pass

    template = "sketch.html"

    experiments = get_experiments(ELASTICSEARCH_CLIENT)

    context = {
        "request": request,
        "default_experiment": default_experiment
        if default_experiment is not None
        else "concreteness",
        "experiments": experiments,
    }
    return templates.TemplateResponse(template, context)


async def valid_webhook_request(
    websocket: WebSocket, request: Dict[str, Any], required_keys: List[str]
) -> bool:
    valid = True
    missing = list()
    for required_key in required_keys:
        if required_key not in request:
            valid = False
            missing.append(required_key)

    if not valid:
        await websocket.send_json(
            {"status": 400, "message": "missing required keys", "missing": missing}
        )
    return valid


@app.websocket_route("/data")
async def experiments_listener(websocket: WebSocket):
    experiments = get_experiments(ELASTICSEARCH_CLIENT)

    await websocket.accept()
    request = await websocket.receive_json()

    if valid_webhook_request(websocket, request, ["action"]):
        if request["action"] == "get":
            if valid_webhook_request(
                websocket, request, required_keys=["experiment", "get"]
            ):
                experiment = Experiment(
                    bucket_name=S3_BUCKET,
                    elasticsearch_client=ELASTICSEARCH_CLIENT,
                    local_data_store=Path("static/data"),
                    name=request["experiment"],
                    s3_client=S3_CLIENT,
                )
                try:
                    found = [
                        {
                            "doc": doc,
                            "image_bytes": base64.b64encode(
                                img_path.read_bytes()
                            ).decode("utf-8"),
                        }
                        for doc, img_path in experiment.get(request["get"])
                    ]
                except FileNotFoundError as e:
                    log.info(f"no match for get {e}")
                    await websocket.send_json(
                        {
                            "status": 404,
                            "message": "no colorgram for search term",
                            "query": request["get"],
                        }
                    )
                    return

                resp = {"status": 200, "found": found[0]}
                clean_resp = copy.deepcopy(resp)
                del clean_resp["found"]["doc"]["_source"]["downloads"]
                del clean_resp["found"]["image_bytes"]
                log.info(f"sending JSON response through websocket: {clean_resp}")
                await websocket.send_json(resp)
        elif request["action"] == "list_experiments":
            await websocket.send_json(
                {"status": 200, "experiments": list(experiments.keys())}
            )
        else:
            await websocket.send_json(
                {"status": 404, "message": f"no action found for {request['action']}"}
            )


@app.route("/langip_grids_{langip_name}")
async def langip_grids(request: Request):
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
        "raw_data_link": get_raw_data_link(experiment),
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
        status_code = 200
    except FileNotFoundError as e:
        log.error(f"{experiment_name}: {e}")
        status_code = 404
        response = {
            "missing": experiment_name,
            "inventory": [
                csv.stem
                for csv in STATIC.joinpath("csv/experiments/").glob("*")
                if csv.suffix == ".csv"
            ],
        }
    except KeyError as e:
        log.error(f"{experiment_name}: {e}")
        status_code = 422
        response = {
            "invalid": experiment_name,
            "message": f"{experiment_name}.csv was found, but is missing a required column: {e}",
        }

    return JSONResponse(response, status_code=status_code)


@app.route("/results/{experiment}")
async def generated(request: Request):
    template = "results.html"

    colorgrams = defaultdict(dict)
    experiment = Path(request["path"]).name
    try:
        all_colorgrams = sorted(
            [
                f.stem
                for f in Path(__file__)
                .parent.joinpath(f"static/img/colorgrams/{experiment}")
                .iterdir()
            ]
        )
    except FileNotFoundError as e:
        return await respond_with_404(request=request, message=str(e))

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
        "raw_data_link": get_raw_data_link(experiment),
    }
    return templates.TemplateResponse(template, context)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    get_elasticsearch_args(parser)
    get_s3_args(parser)

    args = parser.parse_args()

    global ELASTICSEARCH_CLIENT
    global S3_BUCKET
    global S3_CLIENT
    ELASTICSEARCH_CLIENT, S3_CLIENT = get_clients(args)
    S3_BUCKET = args.s3_bucket

    uvicorn.run(app, host="127.0.0.1", port=8080)
