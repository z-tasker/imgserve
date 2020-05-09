poetry run ./bin/colorgrams.py \
  --elasticsearch-client-fqdn "${ES_CLIENT_FQDN}" \
  --elasticsearch-username "${ES_USERNAME}" \
  --elasticsearch-password "${ES_PASSWORD}" \
  --elasticsearch-ca-certs "${ES_CA_CERTS}" \
  --s3-bucket "${AWS_BUCKET_NAME}" \
  --s3-region-name "${AWS_REGION_NAME}" \
  --s3-access-key-id "${AWS_ACCESS_KEY_ID}" \
  --s3-secret-access-key "${AWS_SECRET_ACCESS_KEY}" \
  --local-data-store "${IMGSERVE_DATA_STORE}" \
  "${@}"
