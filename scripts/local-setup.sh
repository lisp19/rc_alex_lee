#!/bin/sh
set -eu
# Generate local credentials and runtime configuration without overwriting files.
test -f configs/management.example.json || { printf '%s\n' 'run from repository root' >&2; exit 1; }
test ! -e .env || { printf '%s\n' '.env already exists; retain existing credentials' >&2; exit 1; }
test ! -e configs/secrets || { printf '%s\n' 'configs/secrets already exists; retain existing keys' >&2; exit 1; }
command -v openssl > /dev/null
command -v python3 > /dev/null
management_source=${1:-configs/management.example.json}
test -f "$management_source"
umask 077
mkdir -p configs/secrets
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out configs/secrets/jwt-private.pem
openssl pkey -in configs/secrets/jwt-private.pem -pubout -out configs/secrets/jwt-public.pem
python3 - "$management_source" <<'PY'
import json, pathlib, secrets, sys
root = pathlib.Path('.')
management = json.loads(pathlib.Path(sys.argv[1]).read_text())
management['issuers'] = {'internal-auth': {'audience':'notification-service', 'algorithm':'RS256', 'public_keys': {'local':(root/'configs/secrets/jwt-public.pem').read_text()}}}
(root/'configs/secrets/management.json').write_text(json.dumps(management, indent=2)+'\n')
process = json.loads((root/'configs/notifier.example.json').read_text())
process['health_listen'] = ':8081'
(root/'configs/secrets/notifier.json').write_text(json.dumps(process, indent=2)+'\n')
names = ['DB_ROOT_PASSWORD','DB_BUSINESS_PASSWORD','DB_CONTROL_PASSWORD','DB_ADMIN_PASSWORD','AMQP_PASSWORD']
(root/'.env').write_text(''.join(f'{name}={secrets.token_hex(24)}\n' for name in names))
PY
# Public documents must be readable by container nonroot; private key and .env
# remain mode 0600 on the host.
chmod 755 configs/secrets
chmod 644 configs/secrets/management.json configs/secrets/notifier.json configs/secrets/jwt-public.pem
printf '%s\n' 'local configuration generated; no dependencies started'
