#!/usr/bin/env python3
"""Apply the versioned smoke configuration and wait for both instances to load it."""
import json
import pathlib
import time

from runtime import compose, database
from health import health

path = pathlib.Path("configs/secrets/management.json")
document = json.loads(path.read_text())
example = json.loads(pathlib.Path("configs/management.example.json").read_text())
for section in ("clients", "targets", "retry_policies", "quota_policies", "hooks"):
    document[section].update(example[section])
# Issuer keys remain those generated for this installation.
path.write_text(json.dumps(document, indent=2)+"\n")
compose("run", "--rm", "--no-deps", "config-init")
revision = int(database("SELECT revision FROM config_revision WHERE scope='global'", "control"))
deadline = time.monotonic()+30
while True:
    try:
        assert all(item["config_revision"] == revision for item in health())
        break
    except Exception:
        if time.monotonic() > deadline: raise
        time.sleep(.5)
print(f"Business contexts applied; both instances converged to revision {revision}")
