#!/usr/bin/env python3
from __future__ import annotations
from pathlib import Path

from imgserve.logger import simple_logger

class MissingJZAZBZArrayError(Exception):
    pass

class MissingRequiredEnvError(Exception):
    pass


def interactive_init() -> None:
    env_file = Path(__file__).parents[1].joinpath(".env")
    env_stub_file = Path(__file__).parents[1].joinpath("env-stub")

    log = simple_logger("imgserve init")

    if not env_file.is_file():
        env_vars = list()
        for line in env_stub_file.read_text().split("\n"):
            variable, default = line.replace("export ", "").split("=")
            value = input(f"enter value for {variable}: " + "(blank for {default})" if default != "" else "")
            if value == "":
                if default == "":
                    raise MissingRequiredEnvError(f"must set {variable}!")
                value = default
                env_vars.append(f"export {variable}={value}")

        env_file.write_text("\n".join(env_vars))

    else:
        log.info(f"{env_file} already exists, leaving it alone")


    imgserve_root = Path(__file__).parents[1].resolve()
    jzazbz_array_path = imgserve_root.joinpath("jzazbz_array.npy")
    if not jzazbz_array_path.is_file():
        raise MissingJZAZBZArrayError(f"please place a copy of jzazbz_array.npy here: {imgserve_root}. You can obtain this file here: https://drive.google.com/file/d/1wspjIBzzvO-ZQbiQs3jgN4UETMxTVD2c/view?usp=sharing, or maybe you already have it on your computer if you run compsyn")
    else:
        log.info("jzazbz_array.npy exists")

    log.info("init successful! Run `source .env` to prepare environment variables for quickstart scripts")


if __name__ == "__main__":
    interactive_init()
