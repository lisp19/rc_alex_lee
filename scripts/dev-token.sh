#!/bin/sh
set -eu
test -f configs/secrets/jwt-private.pem || { printf '%s\n' 'run local-setup first' >&2; exit 1; }
input=$(python3 - <<'PY'
import base64, json, time
def enc(v): return base64.urlsafe_b64encode(json.dumps(v,separators=(',',':')).encode()).decode().rstrip('=')
print(enc({'alg':'RS256','typ':'JWT','kid':'local'})+'.'+enc({'iss':'internal-auth','sub':'demo-service','aud':'notification-service','exp':int(time.time())+3600,'client_id':'demo-client'}))
PY
)
signature=$(printf '%s' "$input" | openssl dgst -sha256 -sign configs/secrets/jwt-private.pem | openssl base64 -A | tr '+/' '-_' | tr -d '=')
printf '%s.%s\n' "$input" "$signature"
