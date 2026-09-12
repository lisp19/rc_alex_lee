#!/usr/bin/env python3
"""Real two-instance E2E acceptance. Run after deploy-local and seed-smoke.

Only generated local fixtures are used. Raw evidence goes to ignored .runtime/.
This is an integration smoke runner, not a mocked implementation/unit test.
"""
import concurrent.futures
import datetime
import json
import os
import time
import uuid

from health import health
from runtime import compose, database, rabbit, request, save, token

RUN = "smoke-" + uuid.uuid4().hex[:12]
START = datetime.datetime.now(datetime.timezone.utc).isoformat()
AUTH = {"Authorization": "Bearer " + token()}
MOCK = "http://127.0.0.1:" + os.environ.get("NOTIFIER_MOCK_PORT", "18090")
results, notifications = [], []


def api(path, method="GET", body=None, instance=0, headers=None):
    return request(f"http://127.0.0.1:{8080+2*instance}/api/v1/"+path, method, body, AUTH if headers is None else headers)


def submission(target="target-a", path="/a", query=None, mode="async"):
    return {"target": target, "delivery_mode": mode, "request": {"method": "POST", "path": path, "headers": {"Content-Type": "application/json"}, "query": query or {}, "body": json.dumps({"event": "inventory.reserved", "order_id": RUN, "sku": "SKU-001", "quantity": 1})}}


def submit(key, body=None, instance=0, auth=None):
    return api("notifications", "POST", body or submission(), instance, {**(auth or AUTH), "Idempotency-Key": RUN+"-"+key})


def accepted(key, body=None, instance=0):
    code, value = submit(key, body, instance)
    assert code in (200, 202), (code, value)
    notifications.append(value["notification_id"])
    return value["notification_id"]


def wait(identifier, state="delivered", timeout=100):
    deadline = time.monotonic()+timeout
    while time.monotonic() < deadline:
        status, value = api("notifications/"+identifier, instance=1)
        assert status == 200, (status, value)
        if value["status"] == state:
            return value
        assert value["status"] not in ("failed", "delivered"), value
        time.sleep(.3)
    raise AssertionError(("notification timeout", identifier, value))


def observe(key):
    status, records = request(MOCK+"/_admin/requests/"+key)
    assert status == 200
    return records or []


def passed(name, evidence=None):
    results.append({"scenario": name, "passed": True, "evidence": evidence})
    save("smoke-results.json", {"run_id": RUN, "started_at": START, "results": results, "notification_ids": notifications})
    print("PASS", name, flush=True)


def main():
    initial = health()
    assert database("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='notify_business'") == "4"
    assert database("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='notify_control'", "control") == "2"
    denied = database("UPDATE config_revision SET revision=revision WHERE scope='global'", "control", False)
    assert denied.returncode != 0 and "denied" in denied.stderr.lower(), "control runtime unexpectedly has write permission"
    denied = database("SELECT * FROM notify_control.config_revision", "business", False)
    assert denied.returncode != 0 and "denied" in denied.stderr.lower(), "business account crosses DB boundary"
    passed("two healthy instances and DB account isolation", initial)

    queues = rabbit("queues/%2F")
    assert len(queues) == 9 and all(q["type"] == "quorum" and q["durable"] for q in queues)
    consumers = rabbit("consumers/%2F")
    assert len(consumers) == 2 and all(c["ack_required"] for c in consumers)
    for q in queues:
        if q["name"].startswith("notify.retry."):
            assert q["arguments"]["x-message-ttl"] > 0
            assert q["arguments"]["x-dead-letter-exchange"] == "notify.dispatch.x"
    passed("quorum topology, seven delay buckets, two manual-ACK consumers", [q["name"] for q in queues])

    assert api("notifications", headers={})[0] == 401
    assert api("notifications", headers={"Authorization": "Bearer "+token(audience="wrong")})[0] == 401
    passed("JWT missing token and audience rejection")

    identifier = accepted("inventory")
    value = wait(identifier)
    assert value["attempt_count"] == 1
    assert "request" not in value and "headers" not in value
    assert submit("inventory", instance=1)[1]["notification_id"] == identifier
    changed = submission(); changed["request"]["body"] = "{}"
    assert submit("inventory", changed, instance=1)[0] == 409
    passed("cross-instance query, idempotent replay and conflicting payload", identifier)

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        raced = list(pool.map(lambda i: submit("race", instance=i % 2), range(8)))
    assert all(c in (200, 202) for c, _ in raced), raced
    ids = {v["notification_id"] for _, v in raced}
    assert len(ids) == 1
    value = wait(ids.pop()); notifications.append(value["notification_id"])
    assert value["attempt_count"] == 1 and len(observe(RUN+"-race")) == 1
    passed("eight concurrent submissions across both instances execute once")

    audit = {"Authorization": "Bearer "+token("audit-client")}
    assert api("notifications/"+identifier, headers=audit)[0] == 404
    assert api("notifications/"+identifier+":retry", "POST", headers=audit)[0] == 403
    assert submit("ssrf", submission(path="/../admin"))[0] == 400
    assert submit("host", submission(path="https://127.0.0.1/admin"))[0] == 400
    passed("client ownership, retry permission and relative-path enforcement")

    item = {**submission(), "idempotency_key": RUN+"-batch"}
    code, batch = api("notifications:batch", "POST", {"items": [item, {**item, "idempotency_key": RUN+"-invalid", "target": "missing-target"}]}, instance=1)
    assert code == 202 and batch["results"][0]["accepted"] and not batch["results"][1]["accepted"], batch
    batch_id = batch["results"][0]["notification_id"]; notifications.append(batch_id); wait(batch_id)
    code, page = api("notifications?batch_id="+batch["batch_id"])
    assert code == 200 and [x["notification_id"] for x in page["items"]] == [batch_id]
    passed("partial batch acceptance and batch correlation", batch)

    retry_id = accepted("retry-5xx", submission(query={"failures": "1"}))
    crm_id = accepted("crm-hook", submission("target-b", "/b", {"failures": "1"}), instance=1)
    limit_id = accepted("ad-rate", submission("target-d", "/d", {"failures": "1", "retry_after": "5"}))
    terminal_id = accepted("crm-terminal", submission("target-b", "/b", {"terminal": "true"}))
    unknown_id = accepted("settlement-reset", submission("target-c", "/c", {"reset": "true"}), instance=1)
    for scenario, current in (("5xx retry and recovery", retry_id), ("CEL CRM business retry and success", crm_id), ("429 Retry-After not-before", limit_id)):
        value = wait(current)
        assert value["attempt_count"] == 2, value
        attempts = list(reversed(value["attempts"]))
        gap = (datetime.datetime.fromisoformat(attempts[1]["started_at"].replace("Z", "+00:00"))-datetime.datetime.fromisoformat(attempts[0]["finished_at"].replace("Z", "+00:00"))).total_seconds()
        assert gap >= 5, gap
        passed(scenario, {"id": current, "retry_gap_seconds": gap, "attempts": value["attempts"]})
    records = observe(RUN+"-crm-hook")
    assert len(records) == 2 and all(r["body_request_id"] == RUN+"-crm-hook" for r in records)
    passed("stable downstream Header and JSON Pointer idempotency injection", records)

    terminal = wait(terminal_id, "failed")
    code, pending = api("notifications/"+terminal_id+":retry", "POST", instance=1)
    assert code == 202
    final = wait(terminal_id, "failed")
    assert final["attempt_count"] == 2 and final["dispatch_generation"] > terminal["dispatch_generation"]
    assert len(observe(RUN+"-crm-terminal")) == 2
    passed("CEL terminal failure and controlled manual retry", final)
    unknown = wait(unknown_id, "failed")
    assert unknown["attempt_count"] == 1 and unknown["last_error_code"] == "uncertain_retry_disabled", unknown
    passed("non-idempotent settlement reset stops automatic retry", unknown)

    proxy = accepted("proxy", submission(mode="proxy"), instance=1)
    assert wait(proxy)["attempt_count"] == 1
    passed("durable proxy delivery", proxy)

    limited_auth = {"Authorization": "Bearer "+token("limited-client")}
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        limited = list(pool.map(lambda i: submit("ingress-"+str(i), instance=i, auth=limited_auth), range(2)))
    assert sorted(c for c, _ in limited) == [202, 429], limited
    passed("Redis ingress quota shared across instances")

    ids = [accepted("egress-"+str(i), submission("target-limited"), instance=i % 2) for i in range(3)]
    for i, current in enumerate(ids):
        value = wait(current)
        assert value["attempt_count"] == 1, value
    received = [observe(RUN+"-egress-"+str(i))[0]["received_at"][:19] for i in range(3)]
    assert len(set(received)) == 3, received
    passed("distributed egress quota defers without consuming attempts", received)

    code, page = api("notifications?limit=2")
    assert code == 200 and len(page["items"]) == 2 and page["next_cursor"]
    code, page2 = api("notifications?limit=2&cursor="+page["next_cursor"], instance=1)
    assert code == 200 and not ({x["notification_id"] for x in page["items"]} & {x["notification_id"] for x in page2["items"]})
    passed("cross-instance cursor pagination")

    compose("stop", "notifier-2")
    try:
        current = accepted("survivor", instance=0)
        deadline = time.monotonic()+30
        while time.monotonic() < deadline:
            _, value = api("notifications/"+current, instance=0)
            if value["status"] == "delivered": break
            time.sleep(.3)
        assert value["status"] == "delivered", value
    finally:
        compose("start", "notifier-2")
    deadline = time.monotonic()+60
    while True:
        try:
            health(); break
        except Exception:
            if time.monotonic() > deadline: raise
            time.sleep(1)
    passed("graceful single-instance stop, survivor delivery and rejoin")

    participation = {}
    for service in ("notifier", "notifier-2"):
        output = compose("logs", "--no-color", "--no-log-prefix", "--since", START, service)
        delivered = []
        for line in output.splitlines():
            try: entry = json.loads(line)
            except json.JSONDecodeError: continue
            if entry.get("msg") == "delivery_finished" and entry.get("result") == "success": delivered.append(entry["notification_id"])
        assert delivered, (service, "no successful worker deliveries")
        participation[service] = delivered
    passed("both instance workers performed successful deliveries", participation)
    key_prefix = RUN+"-%"
    duplicate = database(f"SELECT COUNT(*) FROM (SELECT client_id,client_idem_key,COUNT(*) n FROM notification_task WHERE client_idem_key LIKE '{key_prefix}' GROUP BY client_id,client_idem_key HAVING n>1) d")
    assert duplicate == "0"
    assert database(f"SELECT COUNT(*) FROM delivery_attempt a JOIN notification_task n ON n.id=a.notification_id WHERE n.client_idem_key LIKE '{key_prefix}' AND a.finished_at IS NULL") == "0"
    passed("database has no duplicate logical tasks or unfinished attempts")
    save("smoke-results.json", {"run_id": RUN, "started_at": START, "finished_at": datetime.datetime.now(datetime.timezone.utc).isoformat(), "passed": True, "scenario_count": len(results), "results": results, "health": health(), "notification_ids": notifications})
    print(f"All {len(results)} scenarios passed; evidence: .runtime/smoke-results.json")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        save("smoke-failure.json", {"run_id": RUN, "passed": False, "completed": results, "error": str(error)})
        raise
