import logging

LINE_FORMAT = (
    "[%(asctime)s] %(pathname)s:%(lineno)d (%(name)s)  %(levelname)s: %(message)s"
)

# TODO: use a json formatter here, and index errors to elasticsearch for analysis/alerting
logging.basicConfig(
    level=logging.DEBUG,
    format=LINE_FORMAT,
    datefmt="%Y-%m-%dT%H:%M:%S",
    filename="/tmp/imgserve.errors.log",
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
