kubectl --namespace imgserve delete secret basic-auth
kubectl --namespace imgserve create secret generic basic-auth --from-file=auth
