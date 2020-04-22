#!/usr/bin/env python3
from __future__ import annotations
import os
import logging
from collections import defaultdict
from pathlib import Path

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import StreamingResponse
from starlette.responses import HTMLResponse
from starlette.routing import Route, Mount, WebSocketRoute
from starlette.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates
from starlette.websockets import WebSocket

from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware

middleware = [
        Middleware(CORSMiddleware, allow_origins=["compsyn.fourtheye.xyz", "compsyn.fourtheye.xyz:443"], allow_headers=["*"], allow_methods=["*"])
]

app = Starlette(middleware=middleware)
app.mount("/static", StaticFiles(directory="static"), name="static")

templates = Jinja2Templates(directory="templates")

log = logging.getLogger()

@app.route("/")
async def search(request: Request):
    template = "search.html"

    experiments = [ p.name for p in Path("static/img/colorgrams").glob("*") ]
    context= {
        "request": request,
        "experiments": experiments
    }
    return templates.TemplateResponse(template, context)




@app.route("/{experiment}")
async def generated(request: Request):
    template = "index.html"

    colorgrams = defaultdict(dict)
    experiment = Path(request["path"]).name
    all_colorgrams = sorted([ f.stem for f in Path(__file__).parent.joinpath(f"static/img/colorgrams/{experiment}").iterdir() ])
    for cg in all_colorgrams:
        dimensions = { dim.split("=")[0]: dim.split("=")[1] for dim in cg.split("|") }
        colorgrams[dimensions["query"]][cg] = ", ".join([ f"{value}" for attr, value in sorted(dimensions.items())])

    queries = list(colorgrams.keys())

    context = {
        "experiment": experiment,
        "request": request,
        "queries": queries,
        "colorgrams": colorgrams
    }
    return templates.TemplateResponse(template, context)

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8080)
