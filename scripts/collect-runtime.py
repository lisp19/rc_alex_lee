#!/usr/bin/env python3
"""Capture operational evidence without expanded env/credentials or SQL bodies."""
import datetime
import json
import pathlib
import subprocess

from health import health
from runtime import compose, database, rabbit, save

folder = pathlib.Path(".runtime")
folder.mkdir(mode=0o700, exist_ok=True)
summary = {"collected_at": datetime.datetime.now(datetime.timezone.utc).isoformat(), "health": health()}
summary["mariadb_version"] = database("SELECT VERSION()")
summary["redis_version"] = next(line.split(":", 1)[1] for line in compose("exec", "-T", "redis", "redis-cli", "INFO", "server").splitlines() if line.startswith("redis_version:"))
summary["rabbitmq_version"] = rabbit("overview")["rabbitmq_version"]
summary["queues"] = [{k: q.get(k) for k in ("name", "type", "durable", "messages_ready", "messages_unacknowledged", "consumers", "arguments")} for q in rabbit("queues/%2F")]
summary["consumers"] = len(rabbit("consumers/%2F"))
summary["notification_counts"] = database("SELECT status,COUNT(*) FROM notification_task GROUP BY status")
summary["outbox_counts"] = database("SELECT status,COUNT(*) FROM mq_outbox GROUP BY status")
summary["images"] = []
for image in ("notifier:local", "mariadb:11.8", "redis:8.10.1", "rabbitmq:4.3.5-management"):
    raw = subprocess.check_output(["docker", "image", "inspect", image, "--format", '{{json .Id}} {{json .RepoDigests}}'], text=True).strip()
    summary["images"].append({"image": image, "identity": raw})
for service in ("notifier", "notifier-2", "mariadb", "redis", "rabbitmq", "config-init"):
    (folder/(service+".log")).write_text(compose("logs", "--no-color", "--no-log-prefix", service))
save("deployment-state.json", summary)
print(json.dumps(summary, indent=2))
