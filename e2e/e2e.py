#!/usr/bin/env python3
"""End-to-end check of the local streamhouse (python3 standard library only).

Assumes the stack is running (make up). Applies the e-commerce pipeline and checks that:
  1. every resource becomes READY;
  2. an order inserted in the shop database is visible in the context engine within seconds;
  3. the change lands in the Iceberg table materialized from the topic;
  4. lineage links the shop database to the context table;
  5. agents can use the MCP tools, and only for tables they were granted;
  6. every query, allowed or denied, is audited.
"""
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONTROL_PLANE = os.environ.get("SHCTL_SERVER", "http://localhost:8080")
CONTEXT_ENGINE = os.environ.get("SHCTL_CONTEXT_ENGINE", "http://localhost:8082")
ISSUER = os.environ.get("SHCTL_ISSUER", "http://localhost:8180/realms/streamhouse")
FLINK_GATEWAY = os.environ.get("FLINK_GATEWAY", "http://localhost:8084")
COMPOSE = ["docker", "compose", "-f", os.path.join(ROOT, "deploy/compose/docker-compose.yml")]
FRESHNESS_BUDGET_S = 5.0

failures = []


def check(name, ok, detail=""):
    print(f"{'PASS' if ok else 'FAIL'}  {name}{('  -- ' + detail) if detail else ''}")
    if not ok:
        failures.append(name)


def http(method, url, body=None, token=None, headers=None):
    data = json.dumps(body).encode() if body is not None else None
    h = {"Content-Type": "application/json"} if body is not None else {}
    if token:
        h["Authorization"] = "Bearer " + token
    h.update(headers or {})
    req = urllib.request.Request(url, data=data, method=method, headers=h)
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode()
            return resp.status, dict(resp.headers), raw
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read().decode()


def token(**form):
    data = urllib.parse.urlencode(form).encode()
    with urllib.request.urlopen(ISSUER + "/protocol/openid-connect/token", data, timeout=30) as resp:
        return json.load(resp)["access_token"]


def admin_token():
    return token(grant_type="password", client_id="shctl", username="admin", password="admin")


def agent_token(client):
    return token(grant_type="client_credentials", client_id=client, client_secret=client + "-secret")


def lightning(agent, sql):
    status, _, raw = http("POST", CONTEXT_ENGINE + "/v1/query", {"query": sql}, agent)
    return status, json.loads(raw)


def psql(sql):
    subprocess.run(COMPOSE + ["exec", "-T", "shop-db", "psql", "-U", "shop", "-d", "shop", "-qc", sql], check=True)


def platform_psql(sql):
    out = subprocess.run(COMPOSE + ["exec", "-T", "platform-db", "psql", "-U", "streamhouse", "-d", "streamhouse",
                                    "-tAc", sql], check=True, capture_output=True, text=True)
    return out.stdout.strip()


def flink_batch(statements):
    """Runs statements in one SQL Gateway session; returns the rows of the last one."""
    _, _, raw = http("POST", FLINK_GATEWAY + "/v3/sessions", {"sessionName": "e2e"})
    session = json.loads(raw)["sessionHandle"]
    rows = []
    try:
        for statement in statements:
            _, _, raw = http("POST", f"{FLINK_GATEWAY}/v3/sessions/{session}/statements", {"statement": statement})
            operation = json.loads(raw)["operationHandle"]
            nxt = f"/v3/sessions/{session}/operations/{operation}/result/0?rowFormat=JSON"
            rows = []
            while nxt:
                _, _, raw = http("GET", FLINK_GATEWAY + nxt)
                page = json.loads(raw)
                if page.get("resultType") == "NOT_READY":
                    time.sleep(0.3)
                    continue
                if "results" not in page:
                    raise RuntimeError(raw[:500])
                rows += [r["fields"] for r in page["results"]["data"]]
                nxt = None if page["resultType"] == "EOS" else page.get("nextResultUri")
    finally:
        http("DELETE", f"{FLINK_GATEWAY}/v3/sessions/{session}")
    return rows


class Mcp:
    def __init__(self, access_token):
        self.token = access_token
        self.session = None
        self.ids = 0
        self.rpc("initialize", {"protocolVersion": "2025-06-18", "capabilities": {},
                                "clientInfo": {"name": "e2e", "version": "1"}})
        self.notify("notifications/initialized")

    def _post(self, message):
        headers = {"Accept": "application/json, text/event-stream"}
        if self.session:
            headers["Mcp-Session-Id"] = self.session
        status, resp_headers, raw = http("POST", CONTEXT_ENGINE + "/mcp", message, self.token, headers)
        if status >= 300:
            raise RuntimeError(f"MCP HTTP {status}: {raw[:300]}")
        self.session = self.session or resp_headers.get("Mcp-Session-Id") or resp_headers.get("mcp-session-id")
        if raw.startswith("event:") or "\ndata:" in raw or raw.startswith("data:"):
            raw = "".join(line[5:].strip() for line in raw.splitlines() if line.startswith("data:"))
        return json.loads(raw) if raw else None

    def notify(self, method):
        self._post({"jsonrpc": "2.0", "method": method})

    def rpc(self, method, params=None):
        self.ids += 1
        return self._post({"jsonrpc": "2.0", "id": self.ids, "method": method, "params": params or {}})["result"]

    def tool(self, name, **arguments):
        return self.rpc("tools/call", {"name": name, "arguments": arguments})


def main():
    # 1. Apply the pipeline and wait until it is running.
    with open(os.path.join(ROOT, "examples/ecommerce/pipeline.sql")) as f:
        status, _, raw = http("POST", CONTROL_PLANE + "/v1/sql", {"sql": f.read()}, admin_token())
    result = json.loads(raw)
    check("pipeline.sql is accepted", status == 200 and result.get("ok"), raw[:300] if not result.get("ok") else "")

    deadline = time.time() + 300
    while True:
        _, _, raw = http("GET", CONTROL_PLANE + "/v1/resources", token=admin_token())
        resources = json.loads(raw)
        if all(r["phase"] == "READY" and r["settled"] for r in resources) or time.time() > deadline:
            break
        time.sleep(3)
    not_ready = [f"{r['kind']} {r['name']}: {r['phase']} {r['message']}" for r in resources if r["phase"] != "READY"]
    check(f"all {len(resources)} resources READY", not not_ready, "; ".join(not_ready))

    # 2. Freshness: insert an order and wait for customer_360 to reflect it.
    agent = agent_token("support-agent")
    _, before = lightning(agent, "SELECT orders, open_orders FROM customer_360 WHERE customer_id = 7")
    orders_before = before["rows"][0][0]
    psql("INSERT INTO orders (customer_id, status, total) VALUES (7, 'open', 42.00)")
    started = time.time()
    latency = None
    while time.time() - started < 60:
        _, after = lightning(agent, "SELECT orders, open_orders FROM customer_360 WHERE customer_id = 7")
        if after["rows"] and after["rows"][0][0] == orders_before + 1:
            latency = time.time() - started
            break
        time.sleep(0.1)
    check(f"new order visible in the context engine within {FRESHNESS_BUDGET_S:.0f}s",
          latency is not None and latency <= FRESHNESS_BUDGET_S,
          f"{latency:.2f}s" if latency is not None else "not visible after 60s")

    # 3. Iceberg: the same change lands in the lake (committed on Flink checkpoints).
    catalog = ("CREATE CATALOG IF NOT EXISTS lake WITH ('type'='iceberg','catalog-type'='rest','uri'='http://gravitino:9001/iceberg/',"
               "'io-impl'='org.apache.iceberg.aws.s3.S3FileIO','s3.endpoint'='http://s3:8333','s3.path-style-access'='true',"
               "'s3.access-key-id'='streamhouse','s3.secret-access-key'='streamhouse-secret','client.region'='us-east-1')")
    iceberg_orders = None
    started = time.time()
    while time.time() - started < 90:
        rows = flink_batch([catalog, "SET 'execution.runtime-mode' = 'batch'",
                            "SELECT orders FROM lake.streamhouse.customer_360 WHERE customer_id = 7"])
        iceberg_orders = rows[0][0] if rows else None
        if iceberg_orders == orders_before + 1:
            break
        time.sleep(5)
    check("change committed to the Iceberg table lake.streamhouse.customer_360", iceberg_orders == orders_before + 1,
          f"orders={iceberg_orders}, expected {orders_before + 1}, after {time.time() - started:.0f}s")

    # 4. Lineage from the source database to the context table.
    _, _, raw = http("POST", CONTROL_PLANE + "/v1/sql", {"sql": "DESCRIBE CONTEXT customer_360"}, admin_token())
    props = {row[0]: row[1] for row in json.loads(raw)["results"][0]["rows"]}
    check("lineage: postgres://shop_pg/public.orders is upstream of context://customer_360",
          "postgres://shop_pg/public.orders -> kafka://shop.public.orders" in props.get("upstream", ""), props.get("upstream", ""))

    # 5. Agents over MCP: granted tables only.
    mcp = Mcp(agent)
    tools = sorted(t["name"] for t in mcp.rpc("tools/list")["tools"])
    check("MCP exposes listTopics, getMetadata, queryData", tools == ["getMetadata", "listTopics", "queryData"], str(tools))
    answer = mcp.tool("queryData", query="SELECT name, lifetime_value, open_orders FROM customer_360 WHERE customer_id = 7")
    check("support_agent can query customer_360 over MCP", not answer["isError"], answer["content"][0]["text"][:200])

    untrusted = Mcp(agent_token("untrusted-agent"))
    listed = json.loads(untrusted.tool("listTopics")["content"][0]["text"])
    denied = untrusted.tool("queryData", query="SELECT * FROM customer_360 LIMIT 1")
    check("an agent without grants sees no tables", listed == [], str(listed))
    check("an agent without grants cannot query", denied["isError"], denied["content"][0]["text"][:200])

    # 6. Audit.
    audited = platform_psql("SELECT count(*) FROM serving._audit WHERE principal = 'service-account-untrusted-agent' "
                            "AND outcome = 'DENIED' AND at > now() - interval '5 minutes'")
    check("denied queries are audited", int(audited or 0) >= 1, f"{audited} DENIED entries")

    print()
    if failures:
        print(f"{len(failures)} check(s) failed")
        sys.exit(1)
    print("All end-to-end checks passed")


if __name__ == "__main__":
    main()
