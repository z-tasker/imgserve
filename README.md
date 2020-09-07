# imgserve

The goal of this program is to help automate the process of image collection and analysis.

## requirements

1. `python3`: https://www.python.org/downloads/
2. `poetry`: https://python-poetry.org/docs/
3. `docker`: https://docs.docker.com/get-docker/

## quickstart

NOTE: quickstart will only work on Unix systems like Mac OS and Debian, but this code should run on any system that can satisfy the requirements listed above.

1. satisfy requirements
2. run `poetry run ./bin/init.py`
3. run `source .env`

NOTE: you must run `source .env` every time you come to this folder in a new shell, it sets up secret values in your command line environment required for interacting with S3, Elasticsearch and the central imgserve server.

to contribute results to a given <experiment_name>:

```
./experiment.sh \
  --experiment-name null-test \
  --run-trial
```

To assemble data for a given set of trials for the experiment:

```
./experiment.sh \
  --experiment-name null-test \
  --dimensions query trial_id trial_timestamp \
  --trial-ids test-host-0
```

This command will create a compsyn `downloads` folder for each set of images accross the requested dimensions.

These shell scripts are light wrappers around poetry calls, mostly to keep the number of arguments required to a minimum. They are meant to make the basic usage of this program very simple, but are not required.

## Experiment

This program facilitates the execution of "experiments". Each experiment is logically oriented around a particular hypothesis, and has a unique `experiment_name` associated with it. Each `experiment_name` has 1 or more `trial_id` values associated with it. 

For example: to do a time trial experiment, one would run the experiment code with the `--run-trial` flag every interval, holding the `--experiment-name` value constant. Each run can be differentiated by the `trial_timestamp` field, which gets added automatically by the trial runner code. This could be run accross a set of hosts, and the automatically added hostname used to split results by host, if interested in regional analysis.

The queries to run for each experiment are stored in csv format. By default, this program fetches experiment csvs from the shared remote url, using configuration values from the `.env` file. If you would like to develop your own experiments locally, you can do so by adding the option `--remote-url http://localhost:8080` to your calls to `experiment.py`, this will serve csvs locally, but you must start the imgserve server first:

## Running the imgserve server

This repository also includes a web application for viewing results in the browser, to run it:

`cd ./app && poetry run ./server.py`

This will start a listener at `localhost:8080`. Images and experiment csvs are served out of the `IMGSERVE_LOCAL_DATA_STORE` path configured in `.env`. To start developing a new experiment, create a .csv file at `${IMGSERVE_LOCAL_DATA_STORE}/imgserve/experiments/<experiment_name>.csv`. This file must have the following headers: `search_term`, `region`. Any additional columns will be associated with the raw data results.

open `localhost:8080` in your browser to view experiment results.


## Restoring from Archive

```
./experiment.sh --from-archive-path /Volumes/LACIE/compsyn/data/alpha-archives/langip-grids-emotions --experiment-name langip-grids-emotions
```

## Running LangIP

```
./experiment.sh --experiment-name langip-grids-emotions --trial-ids archive-langip-grids-emotions --dimensions query eng_ref language region experiment_name trial_timestamp
```

# Advanced Trial Runner Options

## Extracting Faces

The trial runner supports face extraction from raw images. Extracted faces are stored as documents in elasticsearch in the `CROPPED_FACE_INDEX_PATTERN`.


```
./experiment.sh \
  --experiment-name null-test \
  --run-trial \
  --extract-faces
```

## Running MTurk

The trial runner supports creation of Mturk Human Intelligence Tasks (HITs). The system first indexes HIT document representations in Elasticsearch, and can also be configured to create HITs in Mturk at query time.

The MTurk HIT template used by this program will be passed the following HITLayoutParameters:
- `image_url` # to present to the worker
- `search_term` # the term used in the search to retrieve this image

The trial run may produce a number of image artifacts (`raw-images`, `colorgrams`, `cropped-face-images`), mturk can be configured to run on any of these by passing the corresponding `--create-mturk-{index_pattern}-hits` flag.

NOTE: at this time, only `--create-mturk-cropped-face-images-hits` is implemented.

```
./experiment.sh \
  --experiment-name face-bias \
  --run-trial \
  --extract-faces \
  --create-mturk-cropped-face-images-hits \
  --mturk-cropped-face-images-hit-type-id ${MTURK_HIT_TYPE_ID} \
  --mturk-cropped-face-images-hit-layout-id ${MTURK_HIT_LAYOUT_ID} \
```

Add the `--mturk-in-realtime` flag to also create HIT objects in Mturk at query time (as opposed to only creating documents). 

By default, mturk client will use the same credentials and bucket name as the S3 Client, but you can override this and use a different set of parameters for mturk using the following flags:

```
--mturk-s3-bucket-name <str>
--mturk-access-key-id <str>
--mturk-secret-access-key <str>
```
