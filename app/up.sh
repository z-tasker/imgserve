#!/bin/bash -e

git pull

~/.poetry/bin/poetry run ./server.py \
  --elasticsearch-client-fqdn "${ES_CLIENT_FQDN}" \
  --elasticsearch-username "${ES_USERNAME}" \
  --elasticsearch-password "${ES_PASSWORD}" \
  --s3-bucket "${AWS_BUCKET_NAME}" \
  --s3-region-name "${AWS_REGION_NAME}" \
  --s3-access-key-id "${AWS_ACCESS_KEY_ID}" \
  --s3-secret-access-key "${AWS_SECRET_ACCESS_KEY}" \
  "${@}"
