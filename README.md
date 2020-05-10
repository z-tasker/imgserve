# imgserve

The goal of this program is to help automate the process of image collection and analysis.

## requirements

`poetry`: https://python-poetry.org/docs/
`docker`: https://docs.docker.com/get-docker/
`python3`: https://www.python.org/downloads/

## quickstart

NOTE: quickstart will only work on Unix systems like Mac OS and Debian, but this code should run on any system that can satisfy the requirements.

1. satisfy requirements
2. run `poetry run ./bin/init.py`
3. run `source .env`

NOTE: you must run `source .env` every time you come to this folder in a new shell, it sets up secret values in your command line environment.

to contribute results to a given <experiment_name>:

```
./experiment.sh --trial-ids ${IMGSERVE_HOSTNAME}  --run-trial --experiment-name null-test
```

to look at colorgrams for all trials run from this host:

```
./experiment.sh --trial-ids ${IMGSERVE_HOSTNAME} --experiment-name null-test --dimensions query ran_at
```

These shell scripts are light wrappers around poetry calls, mostly to keep the number of arguments required to a minimum. They are meant to make the basic usage of this program very simple, but are not required.


