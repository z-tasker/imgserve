#!/bin/bash -e

git pull

ls -lah /home/admin/

#${HOME}/.poetry/bin/poetry run ./bin/experiment.py \
#  --elasticsearch-client-fqdn "${ES_CLIENT_FQDN}" \
#  --elasticsearch-username "${ES_USERNAME}" \
#  --elasticsearch-password "${ES_PASSWORD}" \
#  --remote-username "${IMGSERVE_REMOTE_USERNAME}" \
#  --remote-password "${IMGSERVE_REMOTE_PASSWORD}" \
#  --s3-bucket "${AWS_BUCKET_NAME}" \
#  --s3-region-name "${AWS_REGION_NAME}" \
#  --s3-access-key-id "${AWS_ACCESS_KEY_ID}" \
#  --s3-secret-access-key "${AWS_SECRET_ACCESS_KEY}" \
#  --local-data-store "${IMGSERVE_LOCAL_DATA_STORE}" \
#  "${@}"
