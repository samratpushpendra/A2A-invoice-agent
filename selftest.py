"""Protocol smoke test. Works with LLM_PROVIDER=mock (no API key needed).
   BASE=http://localhost:8000/a2a python selftest.py"""
import os, uuid, httpx

B = os.environ.get("BASE", "http://localhost:8000/a2a").rstrip("/")
ORIGIN = B.split("/", 3)[0] + "//" + B.split("/", 3)[2]
H = {"Authorization": "Bearer user-one", "A2A-Version": "1.0",
     "Content-Type": "application/a2a+json", "Accept": "application/a2a+json"}
OUT = {**H, "Authorization": "Bearer outsider"}
MT_B = "application/vnd.ga5.invoice-claim-batch+json"
MT_R = "application/vnd.ga5.invoice-action-results+json"
ok = lambda c, m: print(("PASS " if c else "FAIL ") + m)

pkgs = [{"packageId": f"p{i}", "cover": "[COVER-1]",
         "text": f"Archive example [OLD-{i}]. Decisive: hold until bank check [E{i}-a] [E{i}-b] [E{i}-c]."}
        for i in range(12)]
mid = str(uuid.uuid4())
msg = {"messageId": mid, "role": "ROLE_USER",
       "parts": [{"mediaType": MT_B, "data": {"batchId": "b1", "policyRevision": "r1", "packages": pkgs}}]}
body = {"message": msg, "configuration": {"returnImmediately": False}}

c = httpx.Client(timeout=60)
card = c.get(ORIGIN + "/.well-known/agent-card.json").json()
ok(card["supportedInterfaces"][0]["protocolBinding"] == "HTTP+JSON", "agent card")
ok(c.post(B + "/message:send", json=body, headers={k: v for k, v in H.items() if k != "Authorization"}).status_code in (401, 403), "no auth")
ok(c.post(B + "/message:send", json=body, headers={**H, "A2A-Version": "0.3"}).status_code == 400, "bad version")

r = c.post(B + "/message:send", json=body, headers=H)
t = r.json()["task"]
ok(r.status_code == 200 and t["status"]["state"] == "TASK_STATE_INPUT_REQUIRED", "INPUT_REQUIRED")
props = t["artifacts"][0]["parts"][0]["data"]["proposals"]
ok(len(props) == 12 and len({p["actionId"] for p in props}) == 12, "12 unique proposals")

body2 = {"message": {**msg}, "configuration": {"returnImmediately": True}}
ok(c.post(B + "/message:send", json=body2, headers=H).json()["task"]["id"] == t["id"], "idempotent replay")
changed = {"message": {**msg, "parts": [{"mediaType": MT_B, "data": {"batchId": "b2", "packages": pkgs}}]}}
r = c.post(B + "/message:send", json=changed, headers=H)
ok(r.status_code == 409 and "IDEMPOTENCY_CONFLICT" in r.text, "idempotency conflict")

ok(c.get(f"{B}/tasks/{t['id']}", headers=OUT).status_code in (403, 404), "outsider get blocked")
ok(c.get(f"{B}/tasks", headers=OUT).json() == {"tasks": []}, "outsider list empty")
ok(c.post(f"{B}/tasks/{t['id']}:cancel", headers=OUT).status_code in (403, 404), "outsider cancel blocked")

def cont(results, ctx=t["contextId"]):
    return {"message": {"messageId": str(uuid.uuid4()), "taskId": t["id"], "contextId": ctx, "role": "ROLE_USER",
            "parts": [{"mediaType": MT_R, "data": {"batchId": "b1", "results": results}}]}}

good = [{"packageId": p["packageId"], "actionId": p["actionId"], "action": p["action"],
         "outcome": "ACCEPTED" if i % 2 == 0 else "REJECTED", "receiptNonce": f"nonce-{i}-xyz"}
        for i, p in enumerate(props)]
bad = [{**good[0], "action": "settle_invoice" if props[0]["action"] != "settle_invoice" else "hold_invoice"}]
ok(c.post(B + "/message:send", json=cont(bad), headers=H).status_code == 400, "mismatched action rejected")
ok(c.post(B + "/message:send", json=cont(good, "wrong-ctx"), headers=H).status_code == 400, "wrong context rejected")
ok(c.post(B + "/message:send", json=cont(good), headers=OUT).status_code in (403, 404), "outsider continue blocked")

r = c.post(B + "/message:send", json=cont(good), headers=H)
t2 = r.json()["task"]
ok(t2["status"]["state"] == "TASK_STATE_COMPLETED", "COMPLETED")
ex = t2["artifacts"][-1]["parts"][0]["data"]["executions"]
ok(len(ex) == 6 and all(e["receiptNonce"].startswith("nonce-") for e in ex), "receipts = accepted only")
ok(c.post(f"{B}/tasks/{t['id']}:cancel", headers=H).status_code == 409, "cancel after terminal -> 409")
ok(c.post(B + "/message:send", json=cont(good), headers=H).status_code == 409, "second continuation -> 409")
ok(c.get(f"{B}/tasks/{t['id']}", headers=H).json() == t2, "terminal task unchanged")
ok(t["id"] in [x["id"] for x in c.get(f"{B}/tasks", headers=H).json()["tasks"]], "owner list")
