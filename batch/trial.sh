#!/bin/bash
set -e 
if [ -z "${1}" ] || [ -z "${2}" ]; then
  echo "provide experiment name and batch slice"
  exit 1
fi
experiment_name="${1}"
batch_slice="${2}"
cd /home/admin/imgserve
git pull 
source .env
poetry run ./bin/init.py
./experiment.sh --experiment-name "${experiment_name}" --share-ip-address
set +e

false
while [ $? -ne 0 ]; do
  ./experiment.sh --experiment-name "${experiment_name}" --no-prompt --no-local-data --run-trial --skip-already-searched --batch-slice "${batch_slice}"
done
set -e
