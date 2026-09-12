"""Local deployment helpers. Credentials stay in process memory, never output."""
import base64
import json
import pathlib
import subprocess
import time
import urllib.error
import urllib.request

COMPOSE = ["docker", "compose", "--env-file", ".env", "-f", "deploy/compose.yaml"]


def compose(*args, **kwargs):
    return subprocess.run(COMPOSE + list(args), check=True, text=True, capture_output=True, **kwargs).stdout


def request(url, method="GET", body=None, headers=None):
    headers = dict(headers or {})
    if body is not None:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=None if body is None else json.dumps(body).encode(), method=method, headers=headers)
    try:
        response = urllib.request.urlopen(req, timeout=45)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        raw = response.read()
        return response.status, json.loads(raw) if raw else None


def token(client="demo-client", audience="notification-service"):
    def enc(value):
        return base64.urlsafe_b64encode(json.dumps(value, separators=(",", ":")).encode()).decode().rstrip("=")
    unsigned = enc({"alg": "RS256", "typ": "JWT", "kid": "local"}) + "." + enc({"iss": "internal-auth", "sub": "smoke-suite", "aud": audience, "exp": int(time.time())+3600, "client_id": client})
    signature = subprocess.run(["openssl", "dgst", "-sha256", "-sign", "configs/secrets/jwt-private.pem"], input=unsigned.encode(), check=True, capture_output=True).stdout
    return unsigned + "." + base64.urlsafe_b64encode(signature).decode().rstrip("=")


def database(sql, account="business", check=True):
    user, password, db = {
        "business": ("notify_runtime_business", "DB_BUSINESS_PASSWORD", "notify_business"),
        "control": ("notify_runtime_control", "DB_CONTROL_PASSWORD", "notify_control"),
        "admin": ("notify_admin_control", "DB_ADMIN_PASSWORD", "notify_control"),
    }[account]
    command = COMPOSE + ["exec", "-T", "mariadb", "sh", "-c", f'MYSQL_PWD="${password}" exec mariadb -u{user} -D{db} -N -B -e "$1"', "sh", sql]
    result = subprocess.run(command, check=check, capture_output=True, text=True)
    return result.stdout.strip() if check else result


def rabbit(path):
    values = dict(line.split("=", 1) for line in pathlib.Path(".env").read_text().splitlines() if line and not line.startswith("#"))
    encoded = base64.b64encode(("notifier:"+values["AMQP_PASSWORD"]).encode()).decode()
    status, value = request("http://127.0.0.1:15672/api/"+path, headers={"Authorization": "Basic "+encoded})
    assert status == 200, (path, status)
    return value


def save(name, value):
    folder = pathlib.Path(".runtime")
    folder.mkdir(mode=0o700, exist_ok=True)
    encoded = json.dumps(value, indent=2)+"\n"
    (folder/name).write_text(encoded)
    # Retain earlier failed/passed runs when the stable "latest" path changes.
    if isinstance(value, dict) and "run_id" in value:
        (folder/(value["run_id"]+"-"+name)).write_text(encoded)
