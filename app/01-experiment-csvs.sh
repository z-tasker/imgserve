#!/bin/bash -e 

if [ -z "${1}" ]; then
  echo "provide path to experiments csvs directory"
  exit 1
fi

set +e
kubectl -n imgserve delete configmap experiment-csvs
kubectl -n imgserve create configmap experiment-csvs --from-file="${1}"
