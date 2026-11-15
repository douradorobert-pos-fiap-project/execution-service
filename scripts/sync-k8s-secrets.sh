#!/usr/bin/env bash
set -euo pipefail
set +x

for name in \
  NEW_RELIC_LICENSE_KEY AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN; do
  if [[ -z "${!name:-}" ]]; then
    printf 'Missing required deployment configuration: %s\n' "$name" >&2
    exit 2
  fi
done

kubectl --namespace oficina create secret generic execution-service-secrets \
  --from-literal="NEW_RELIC_LICENSE_KEY=${NEW_RELIC_LICENSE_KEY}" \
  --from-literal="AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}" \
  --from-literal="AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}" \
  --from-literal="AWS_SESSION_TOKEN=${AWS_SESSION_TOKEN}" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null

printf 'Execution Service Kubernetes Secret synchronized\n'
