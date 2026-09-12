#!/bin/sh
set -eu
test -f deploy/compose.yaml || { printf '%s\n' 'run from repository root' >&2; exit 1; }
for tool in docker python3 openssl; do command -v "$tool" > /dev/null; done
docker info > /dev/null
docker compose version
if test ! -e .env; then sh scripts/local-setup.sh; fi
test -f configs/secrets/management.json
test -f configs/secrets/notifier.json
mkdir -p .runtime
chmod 700 .runtime
# Never print the expanded Compose configuration: it contains credentials.
docker compose --env-file .env -f deploy/compose.yaml config --quiet
docker compose --env-file .env -f deploy/compose.yaml pull mariadb redis rabbitmq
docker compose --env-file .env -f deploy/compose.yaml build notifier
docker compose --env-file .env -f deploy/compose.yaml up -d --wait --wait-timeout 240
python3 scripts/health.py
