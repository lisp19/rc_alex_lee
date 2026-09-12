#!/bin/sh
set -eu
# Future local acceptance only. Every operation is explicit.
test -f deploy/compose.yaml || { printf '%s\n' 'run from repository root' >&2; exit 1; }
case "${1:-}" in
  kill-notifier) docker compose --env-file .env -f deploy/compose.yaml kill -s SIGKILL notifier ;;
  start-notifier) docker compose --env-file .env -f deploy/compose.yaml up -d notifier ;;
  restart-rabbitmq) docker compose --env-file .env -f deploy/compose.yaml restart rabbitmq ;;
  stop-redis) docker compose --env-file .env -f deploy/compose.yaml stop redis ;;
  start-redis) docker compose --env-file .env -f deploy/compose.yaml start redis ;;
  stop-mariadb) docker compose --env-file .env -f deploy/compose.yaml stop mariadb ;;
  start-mariadb) docker compose --env-file .env -f deploy/compose.yaml start mariadb ;;
  *) printf '%s\n' 'usage: sh scripts/fault.sh kill-notifier|start-notifier|restart-rabbitmq|stop-redis|start-redis|stop-mariadb|start-mariadb' >&2; exit 2 ;;
esac
