#!/usr/bin/env python3
"""Check both local instances; print only the secret-free health contract."""
import json
import urllib.request


def health():
    results = []
    for port in (8081, 8083):
        for path in ("live", "ready"):
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/health/{path}", timeout=5) as response:
                assert response.status == 200
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/health/detail", timeout=5) as response:
            detail = json.load(response)
        assert detail["ready"] and all(detail[k] for k in ("business_db", "control_db", "rabbitmq", "redis")), detail
        results.append({"port": port, **detail})
    assert len({item["instance_id"] for item in results}) == 2, "expected independent instances"
    assert len({item["config_revision"] for item in results}) == 1, "configuration has not converged"
    return results


if __name__ == "__main__":
    print(json.dumps(health(), indent=2))
