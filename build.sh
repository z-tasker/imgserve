#!/bin/bash -e

if [ -z ${1} ]; then
  echo "provide imgserve image version and then --push (optional)"
  exit 1
fi

IMGSERVE_VERSION=${1}

docker build -t mgraskertheband/imgserve:${IMGSERVE_VERSION} .
if [ "${2}" == "--push" ]; then
  docker push mgraskertheband/imgserve:${IMGSERVE_VERSION} 
fi
cd app 
docker build -t mgraskertheband/imgserve-web:${IMGSERVE_VERSION} --build-arg imgserve_version=${IMGSERVE_VERSION} .
if [ "${2}" == "--push" ]; then
  docker push mgraskertheband/imgserve-web:${IMGSERVE_VERSION} 
fi
cd -

