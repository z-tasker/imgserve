from __future__ import annotations
import logging
from pathlib import Path

DEBUG_FORMAT = (
    "[%(asctime)s] %(pathname)s:%(lineno)d (%(name)s)  %(levelname)s: %(message)s"
)

LINE_FORMAT = "[%(asctime)s] (%(name)s)  %(levelname)s: %(message)s"

# TODO: use a json formatter here, and index errors to elasticsearch for analysis/alerting
logging.basicConfig(
    level=logging.DEBUG,
    format=DEBUG_FORMAT,
    datefmt="%Y-%m-%dT%H:%M:%S",
    filename=Path(__file__).parents[2].joinpath("imgserve.log"),
    filemode="w",
)


def simple_logger(name: str, level: int = logging.INFO) -> logging.Logger:

    logger = logging.getLogger(name)

    if len(logger.handlers) < 2:
        console = logging.StreamHandler()
        console.setLevel(level)
        console.setFormatter(logging.Formatter(LINE_FORMAT))

        logger.addHandler(console)

    return logger
