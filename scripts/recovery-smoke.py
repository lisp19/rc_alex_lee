#!/usr/bin/env python3
"""Bounded dependency/lease recovery checks against this local Compose project.

The script stops only project-owned Redis/RabbitMQ/Notifier containers and
always attempts to start them again. Keep isolated acceptance environments.
"""
import json
import time

from health import health
from runtime import compose, database, request, save
from smoke import RUN, accepted, api, submission, wait

results = []


def eventually(check, timeout=90):
    deadline = time.monotonic()+timeout
    last = None
    while time.monotonic() < deadline:
        try:
            value = check()
            if value:
                return value
        except Exception as error:
            last = str(error)
        time.sleep(.5)
    raise AssertionError(("condition timed out", last))


def healthy():
    return health()


def record(name, data):
    results.append({"scenario": name, "passed": True, "evidence": data})
    save("recovery-results.json", {"run_id": RUN, "passed": len(results) == 3, "results": results})
    print("PASS", name, flush=True)


def main():
    health()
    try:
        compose("stop", "redis")
        details = [request(f"http://127.0.0.1:{p}/health/detail")[1] for p in (8081, 8083)]
        assert all(not d["redis"] and d["quota_degraded"] and d["ready"] for d in details), details
        current = accepted("redis-fail-open")
        delivered = wait(current)
        record("Redis outage: fail-open, DB lease-only delivery, degraded health", delivered)
    finally:
        compose("start", "redis")
    eventually(healthy)

    try:
        compose("stop", "rabbitmq")
        eventually(lambda: all(request(f"http://127.0.0.1:{p}/health/ready")[0] == 503 for p in (8081, 8083)))
        current = accepted("mq-outbox")
        _, pending = api("notifications/"+current)
        assert pending["status"] == "pending" and pending["attempt_count"] == 0, pending
        assert int(database("SELECT COUNT(*) FROM mq_outbox WHERE notification_id=UNHEX('"+current.replace("-", "")+"') AND status IN ('pending','publishing')")) > 0
    finally:
        compose("start", "rabbitmq")
    eventually(healthy)
    delivered = wait(current)
    record("RabbitMQ outage: Not Ready, durable Outbox acceptance, automatic reconnect", delivered)

    current = accepted("lease-kill", submission(query={"sleep": "3"}))
    def claimed():
        _, task = api("notifications/"+current)
        if task["status"] != "in_flight": return None
        for service, port in (("notifier", 8081), ("notifier-2", 8083)):
            detail = request(f"http://127.0.0.1:{port}/health/detail")[1]
            if detail["worker_active"] > 0: return service
        return None
    victim = eventually(claimed, 10)
    started = time.monotonic()
    try:
        compose("kill", "-s", "SIGKILL", victim)
    finally:
        compose("start", victim)
    eventually(healthy)
    delivered = wait(current, timeout=100)
    assert delivered["attempt_count"] == 2, delivered
    attempts = delivered["attempts"]
    assert attempts[0]["result"] == "success" and attempts[1]["result"] == "unknown", attempts
    assert attempts[1]["error_code"] == "lease_expired" and delivered["dispatch_generation"] > 1
    record("kill -9 during HTTP: unknown Attempt, lease recovery and fenced redelivery", {"victim": victim, "recovery_seconds": round(time.monotonic()-started, 3), "notification": delivered})
    health()
    print("All 3 recovery scenarios passed; evidence: .runtime/recovery-results.json")


if __name__ == "__main__":
    main()
