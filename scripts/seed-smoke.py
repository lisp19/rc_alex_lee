#!/usr/bin/env python3
"""Add isolated business contexts through the normal transactional admin path."""
import copy
import json
import pathlib
import time

from runtime import compose, database
from health import health

path = pathlib.Path("configs/secrets/management.json")
document = json.loads(path.read_text())
# A/B/C/D model inventory sync, CRM business decisions, non-idempotent settlement,
# and an advertising endpoint with explicit Retry-After.
document["clients"]["audit-client"] = {"enabled": True, "targets": ["target-a"], "manual_retry": False, "quota_policy": "default"}
document["quota_policies"]["smoke-egress"] = {**document["quota_policies"]["default"], "egress_target": 1}
document["quota_policies"]["smoke-ingress"] = {**document["quota_policies"]["default"], "ingress_client": 1}
limited = copy.deepcopy(document["targets"]["target-a"])
limited["quota_policy"] = "smoke-egress"
document["targets"]["target-limited"] = limited
document["clients"]["limited-client"] = {"enabled": True, "targets": ["target-a"], "manual_retry": False, "quota_policy": "smoke-ingress"}
if "target-limited" not in document["clients"]["demo-client"]["targets"]:
    document["clients"]["demo-client"]["targets"].append("target-limited")
# The file is a generated, ignored deployment input; history remains immutable
# because notify-admin commits a new global revision rather than updating rows.
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
