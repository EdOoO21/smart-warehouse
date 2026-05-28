#!/usr/bin/env python3
import json
import sys
import time
import urllib.parse
import urllib.request

PROMETHEUS = "http://127.0.0.1:9090"

CHECKS = {
    "availability": {
        "query": 'sum(rate(http_requests_total{status=~"2..|3.."}[1m])) / clamp_min(sum(rate(http_requests_total[1m])), 0.000001)',
        "min": 0.995,
    },
    "http_error_rate": {
        "query": "sum(rate(http_request_errors_total[1m])) / clamp_min(sum(rate(http_requests_total[1m])), 0.000001)",
        "max": 0.01,
    },
    "http_p95_latency_seconds": {
        "query": "histogram_quantile(0.95, sum by (le) (rate(http_request_duration_seconds_bucket[1m])))",
        "max": 0.5,
    },
    "event_processing_p95_seconds": {
        "query": "histogram_quantile(0.95, sum by (le) (rate(event_processing_duration_seconds_bucket[1m])))",
        "max": 1.0,
    },
}


def query(promql: str) -> float:
    encoded = urllib.parse.urlencode({"query": promql})
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(f"{PROMETHEUS}/api/v1/query?{encoded}", timeout=10) as response:
        payload = json.loads(response.read().decode())
    if payload["status"] != "success":
        raise RuntimeError(payload)
    result = payload["data"]["result"]
    if not result:
        return 0.0
    return float(result[0]["value"][1])


def main() -> int:
    time.sleep(10)
    report = {}
    failed = False
    for name, check in CHECKS.items():
        value = query(check["query"])
        item = {"value": value, "query": check["query"]}
        if "max" in check:
            item["max"] = check["max"]
            if value > check["max"]:
                failed = True
                item["passed"] = False
            else:
                item["passed"] = True
        if "min" in check:
            item["min"] = check["min"]
            if value < check["min"]:
                failed = True
                item["passed"] = False
            else:
                item["passed"] = True
        report[name] = item

    print(json.dumps(report, indent=2, sort_keys=True))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
