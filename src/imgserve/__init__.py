from __future__ import annotations
import os
from pathlib import Path

LOCAL_DATA_STORE = os.getenv("IMGSERVE_LOCAL_DATA_STORE", None)
assert (
    LOCAL_DATA_STORE is not None
), "set IMGSERVE_LOCAL_DATA_STORE in your environment to point at a local directory for storage"
LOCAL_DATA_STORE = Path(LOCAL_DATA_STORE)

STATIC = Path(__file__).parents[2].joinpath("app/static")
assert (
    STATIC.is_dir()
), f"{STATIC} is not a directory, can not use it as source of static assets"


def get_experiment_colorgrams_path(
    name: str, local_data_store: Path = LOCAL_DATA_STORE, app_static_path: Path = STATIC
) -> Path:
    """
        get path, manage symlink to LOCAL_DATA_STORE
    """
    experiment_path = local_data_store.joinpath("imgserve/colorgrams").joinpath(name)
    experiment_path.mkdir(exist_ok=True, parents=True)

    app_symlink = app_static_path.joinpath("img/colorgrams")
    app_symlink.parent.mkdir(exist_ok=True, parents=True)
    try:
        app_symlink.unlink()
    except FileNotFoundError:
        pass

    app_symlink.symlink_to(experiment_path.parent, target_is_directory=True)

    return experiment_path


def get_experiment_csv_path(
    name: str, local_data_store: Path = LOCAL_DATA_STORE, app_static_path: Path = STATIC
) -> Path:
    """
        get path, Try to fetch from master server if missing. manage symlink to LOCAL_DATA_STORE.
    """
    csv_path = (
        local_data_store.joinpath("imgserve/experiments")
        .joinpath(name)
        .with_suffix(".csv")
    )

    app_symlink = app_static_path.joinpath("csv/experiments")
    app_symlink.parent.mkdir(exist_ok=True, parents=True)
    try:
        app_symlink.unlink()
    except FileNotFoundError:
        pass

    app_symlink.symlink_to(csv_path.parent, target_is_directory=True)

    if not csv_path.is_file():
        raise FileNotFoundError(csv_path)

    return csv_path
