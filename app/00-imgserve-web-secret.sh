kubectl -n imgserve create secret generic imgserve-web --from-env-file <(jq -r "to_entries|map(\"\(.key)=\(.value|tostring)\")|.[]" /Volumes/LACIE/secrets/imgserve-kube-env.json)
