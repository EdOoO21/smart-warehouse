import http from "k6/http";
import { check, sleep } from "k6";

export const options = {
  vus: 10,
  duration: "30s",
  thresholds: {
    http_req_failed: ["rate<0.01"],
    http_req_duration: ["p(95)<500"],
  },
};

export default function () {
  const baseURL = __ENV.BASE_URL || "http://host.docker.internal:8082";
  const id = `${__VU}-${__ITER}-${Date.now()}`;
  const payload = JSON.stringify({
    event_id: `load-${id}`,
    event_type: "PRODUCT_RECEIVED",
    occurred_at: new Date().toISOString(),
    product_id: `LOAD-SKU-${id}`,
    zone_id: "ZONE-A",
    quantity: 1,
    schema_version: 1,
  });
  const response = http.post(`${baseURL}/events`, payload, {
    headers: { "Content-Type": "application/json" },
  });
  check(response, { accepted: (r) => r.status === 202 });
  sleep(0.2);
}
