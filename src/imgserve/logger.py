import logging

logging.basicConfig(
    level=logging.DEBUG,
    format="{%(pathname)s:%(lineno)d} %(asctime)s %(name)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    filename="/tmp/imgserve.errors.log",
    filemode="w",
)


def simple_logger(name: str, level: int = logging.INFO) -> logging.Logger:

    logger = logging.getLogger(name)

    if len(logger.handlers) < 2:
        console = logging.StreamHandler()
        console.setLevel(level)
        console.setFormatter(
            logging.Formatter(
                "%(pathname)s:%(lineno)d [%(asctime)s] (%(name)s)  %(levelname)s: %(message)s"
            )
        )

        logger.addHandler(console)

    return logger
