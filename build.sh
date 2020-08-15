#!/bin/bash -e

docker build -t mgraskertheband/imgserve:${IMGSERVE_VERSION} .
if [ "${1}" == "--push" ]; then
  docker push mgraskertheband/imgserve:${IMGSERVE_VERSION} 
fi
cd app 
docker build -t mgraskertheband/imgserve-web:${IMGSERVE_VERSION} --build-arg imgserve_version=${IMGSERVE_VERSION} .
if [ "${1}" == "--push" ]; then
  docker push mgraskertheband/imgserve-web:${IMGSERVE_VERSION} 
fi
cd -

