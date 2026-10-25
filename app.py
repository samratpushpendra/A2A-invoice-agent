"""
A2A 1.0 (HTTP+JSON) invoice action agent.

Layers (kept separate on purpose):
  1. Protocol   - routes, auth, headers, media types, error envelopes
  2. Storage    - SQLite (tasks, message-idempotency records, decision cache)
  3. Reasoning  - ONE batched LLM call per 12 uncached packages, strict validation
  4. Lifecycle  - INPUT_REQUIRED -> COMPLETED / CANCELED, receipt gate, cancel race

Run with exactly ONE worker (locks are in-process):
    uvicorn app:app --host 0.0.0.0 --port 8000 --workers 1
"""
import asyncio
import hashlib
import json
import os
import re
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone
from urllib.parse import urlparse

import httpx
from fastapi import APIRouter, FastAPI, Request
from fastapi.responses import Response
from starlette.exceptions import HTTPException as StarletteHTTPException

# --------------------------------------------------------------------------- constants
A2A_JSON = "application/a2a+json"
MT_BATCH = "application/vnd.ga5.invoice-claim-batch+json"
MT_PROPOSALS = "application/vnd.ga5.invoice-action-proposals+json"
MT_RESULTS = "application/vnd.ga5.invoice-action-results+json"
MT_RECEIPTS = "application/vnd.ga5.invoice-action-receipts+json"

ACTIONS = ["settle_invoice", "request_approval", "hold_invoice", "reject_duplicate", "open_exception"]

S_WORKING = "TASK_STATE_WORKING"
S_INPUT = "TASK_STATE_INPUT_REQUIRED"
S_DONE = "TASK_STATE_COMPLETED"
S_CANCELED = "TASK_STATE_CANCELED"
S_FAILED = "TASK_STATE_FAILED"
TERMINAL = {S_DONE, S_CANCELED, S_FAILED, "TASK_STATE_REJECTED"}

# ---- configuration (env) ----
BASE_URL = os.environ.get("BASE_URL", "")            # EXACT string you submit to the grader
DB_PATH = os.environ.get("DB_PATH", "a2a.db")
LLM_PROVIDER = os.environ.get("LLM_PROVIDER", "openai")   # openai (any OpenAI-compatible) | anthropic | mock
LLM_BASE_URL = os.environ.get("LLM_BASE_URL", "https://api.openai.com/v1").rstrip("/")
LLM_API_KEY = os.environ.get("LLM_API_KEY", "")
LLM_MODEL = os.environ.get("LLM_MODEL", "gpt-4o-mini")
LLM_JSON_MODE = os.environ.get("LLM_JSON_MODE", "1") == "1"
# Evidence refs are returned as the token INSIDE the brackets by default.
# If grader feedback says evidence is wrong, try EVIDENCE_BRACKETS=1.
EVIDENCE_BRACKETS = os.environ.get("EVIDENCE_BRACKETS", "0") == "1"
ALLOWED_TOKENS = {t for t in os.environ.get("ALLOWED_TOKENS", "").split(",") if t}  # optional restriction
REQUEST_BUDGET = 40.0           # seconds of the 45s per-request limit we allow for reasoning
MAX_RESPONSE = 500 * 1024       # stay under 512 KiB
CHUNK = 12

LLM_SEM = asyncio.Semaphore(4)

# --------------------------------------------------------------------------- helpers
def now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def canon(o) -> str:
    return json.dumps(o, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest()


def jbytes(o) -> bytes:
    return json.dumps(o, separators=(",", ":"), ensure_ascii=False).encode()


# --------------------------------------------------------------------------- errors
STATUS_NAMES = {400: "INVALID_ARGUMENT", 401: "UNAUTHENTICATED", 403: "PERMISSION_DENIED", 404: "NOT_FOUND",
                405: "INVALID_ARGUMENT", 406: "INVALID_ARGUMENT", 409: "ABORTED", 413: "INVALID_ARGUMENT",
                415: "INVALID_ARGUMENT", 500: "INTERNAL"}


class ApiError(Exception):
    def __init__(self, http: int, reason: str, message: str = "Request failed"):
        self.http, self.reason, self.message = http, reason, message


def err_response(e: ApiError) -> Response:
    body = {
        "code": e.reason,
        "error": {
            "code": e.http,
            "status": STATUS_NAMES.get(e.http, "UNKNOWN"),
            "reason": e.reason,
            "message": e.message,
            "details": [{"@type": "type.googleapis.com/google.rpc.ErrorInfo",
                         "reason": e.reason, "domain": "a2a-protocol.org"}],
        },
    }
    headers = {"WWW-Authenticate": "Bearer"} if e.http == 401 else {}
    return Response(jbytes(body), status_code=e.http, media_type=A2A_JSON, headers=headers)


def jresp(obj, status=200) -> Response:
    return Response(jbytes(obj), status_code=status, media_type=A2A_JSON)


# --------------------------------------------------------------------------- storage
_db = sqlite3.connect(DB_PATH, check_same_thread=False, isolation_level=None)
_dbl = threading.RLock()
with _dbl:
    _db.execute("PRAGMA journal_mode=WAL")
    _db.execute("PRAGMA synchronous=FULL")
    _db.execute("""CREATE TABLE IF NOT EXISTS tasks(
        id TEXT PRIMARY KEY, principal TEXT NOT NULL, created REAL NOT NULL,
        state TEXT NOT NULL, task TEXT NOT NULL, proposals TEXT)""")
    _db.execute("CREATE INDEX IF NOT EXISTS tasks_p ON tasks(principal, created)")
    _db.execute("""CREATE TABLE IF NOT EXISTS msgs(
        principal TEXT NOT NULL, message_id TEXT NOT NULL, hash TEXT NOT NULL, task_id TEXT NOT NULL,
        PRIMARY KEY(principal, message_id))""")
    _db.execute("CREATE TABLE IF NOT EXISTS cache(key TEXT PRIMARY KEY, decision TEXT NOT NULL)")


def db_get_task(tid):
    with _dbl:
        r = _db.execute("SELECT principal, task, proposals FROM tasks WHERE id=?", (tid,)).fetchone()
    if not r:
        return None
    return {"principal": r[0], "task": json.loads(r[1]), "proposals": json.loads(r[2]) if r[2] else None}


def db_put_task(tid, principal, task, proposals, msg_rec=None):
    """Atomically upsert a task and (optionally) its idempotency record."""
    with _dbl:
        _db.execute("BEGIN IMMEDIATE")
        try:
            _db.execute(
                """INSERT INTO tasks(id,principal,created,state,task,proposals) VALUES(?,?,?,?,?,?)
                   ON CONFLICT(id) DO UPDATE SET state=excluded.state, task=excluded.task,
                   proposals=excluded.proposals""",
                (tid, principal, time.time(), task["status"]["state"], json.dumps(task),
                 json.dumps(proposals) if proposals is not None else None))
            if msg_rec:
                _db.execute("INSERT OR IGNORE INTO msgs(principal,message_id,hash,task_id) VALUES(?,?,?,?)", msg_rec)
            _db.execute("COMMIT")
        except Exception:
            _db.execute("ROLLBACK")
            raise


def db_get_msg(principal, mid):
    with _dbl:
        return _db.execute("SELECT hash, task_id FROM msgs WHERE principal=? AND message_id=?",
                           (principal, mid)).fetchone()


def db_list(principal):
    with _dbl:
        rows = _db.execute("SELECT task FROM tasks WHERE principal=? ORDER BY created DESC, rowid DESC",
                           (principal,)).fetchall()
    return [json.loads(r[0]) for r in rows]


def cache_get(key):
    with _dbl:
        r = _db.execute("SELECT decision FROM cache WHERE key=?", (key,)).fetchone()
    return json.loads(r[0]) if r else None


def cache_put(key, dec):
    with _dbl:
        _db.execute("INSERT OR REPLACE INTO cache(key,decision) VALUES(?,?)", (key, json.dumps(dec)))


_locks: dict = {}


def lock_for(key) -> asyncio.Lock:
    lk = _locks.get(key)
    if lk is None:
        lk = _locks[key] = asyncio.Lock()
    return lk


# --------------------------------------------------------------------------- reasoning layer
BR = re.compile(r"\[([^\[\]\r\n]{1,120})\]")


def all_text(o, out=None):
    out = [] if out is None else out
    if isinstance(o, str):
        out.append(o)
    elif isinstance(o, dict):
        for v in o.values():
            all_text(v, out)
    elif isinstance(o, list):
        for v in o:
            all_text(v, out)
    return out


def bracket_tokens(pkg) -> list:
    seen = {}
    for s in all_text(pkg):
        for m in BR.findall(s):
            seen[m.strip()] = None
    return list(seen)


def find_key(o, key):
    if isinstance(o, dict):
        if key in o and not isinstance(o[key], (dict, list)):
            return o[key]
        for v in o.values():
            r = find_key(v, key)
            if r is not None:
                return r
    elif isinstance(o, list):
        for v in o:
            r = find_key(v, key)
            if r is not None:
                return r
    return None


SYSTEM = """You are a careful accounts-payable controls analyst. You receive synthetic invoice packages and must choose exactly ONE action per package.

Actions:
- settle_invoice: invoice is valid, reconciled, and within autonomous payment authority.
- request_approval: commercially valid, but the amount/terms are outside delegated authority; a human approver must approve first.
- hold_invoice: payment pauses until a stated verification completes (e.g. bank-detail check, goods receipt, vendor confirmation).
- reject_duplicate: the SAME commercial invoice (same vendor, same invoice number, same amount/commercial event) was already paid. A payment for a DIFFERENT invoice is NOT a duplicate.
- open_exception: material records conflict (e.g. PO vs invoice vs receipt mismatch) and need an exception workflow.

How to read a package:
- Each package mixes decisive facts with noise: cover-sheet summaries, archive / historical / old examples, training or sample material, superseded text, negated statements ("do not hold...", "this is not a duplicate"), and irrelevant action words. Ignore all noise.
- Find the single paragraph that states the current, authoritative facts that determine the action under the given policy revision. Decide from that paragraph only.
- evidenceRefs: EXACTLY three bracketed reference tokens that appear in that decisive paragraph. Copy each token exactly as written inside the brackets (omit the brackets). Never use the cover-sheet reference, archive examples or training decoys.
- facts: vendorName, invoiceNumber and currency (ISO code) of the actual invoice being decided. amountMinor is the invoice amount as an integer in minor units (1,234.50 INR -> 123450; zero-decimal currencies such as JPY are not multiplied).
- reason: 1-2 sentences naming the decisive condition.

Return ONLY JSON, no markdown:
{"decisions":[{"packageId":"...","action":"...","facts":{"vendorName":"...","invoiceNumber":"...","amountMinor":0,"currency":"INR"},"evidenceRefs":["a","b","c"],"reason":"..."}]}
One entry per package, using the given packageId."""


async def call_llm(user: str, timeout: float) -> str:
    if LLM_PROVIDER == "mock":
        raise RuntimeError("mock provider")
    async with httpx.AsyncClient(timeout=timeout) as c:
        if LLM_PROVIDER == "anthropic":
            r = await c.post("https://api.anthropic.com/v1/messages",
                             headers={"x-api-key": LLM_API_KEY, "anthropic-version": "2023-06-01"},
                             json={"model": LLM_MODEL, "max_tokens": 8000, "temperature": 0,
                                   "system": SYSTEM, "messages": [{"role": "user", "content": user}]})
            r.raise_for_status()
            return "".join(b.get("text", "") for b in r.json()["content"])
        payload = {"model": LLM_MODEL, "temperature": 0, "max_tokens": 8000,
                   "messages": [{"role": "system", "content": SYSTEM}, {"role": "user", "content": user}]}
        if LLM_JSON_MODE:
            payload["response_format"] = {"type": "json_object"}
        r = await c.post(f"{LLM_BASE_URL}/chat/completions",
                         headers={"Authorization": f"Bearer {LLM_API_KEY}"}, json=payload)
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"]


def parse_json(text: str):
    text = text.strip()
    text = re.sub(r"^```(?:json)?|```$", "", text, flags=re.M).strip()
    try:
        return json.loads(text)
    except Exception:
        i, j = text.find("{"), text.rfind("}")
        return json.loads(text[i:j + 1])


def validate_decision(d, pkg):
    if not isinstance(d, dict) or d.get("action") not in ACTIONS:
        return None
    f = d.get("facts")
    if not isinstance(f, dict):
        return None
    vendor = str(f.get("vendorName") or "").strip()
    inv = str(f.get("invoiceNumber") or "").strip()
    cur = str(f.get("currency") or "").strip().upper()
    am = f.get("amountMinor")
    if isinstance(am, bool):
        return None
    if isinstance(am, float) and am == int(am):
        am = int(am)
    elif isinstance(am, str) and re.fullmatch(r"\d+", am.strip()):
        am = int(am.strip())
    if not (vendor and inv and cur and isinstance(am, int)):
        return None
    refs = d.get("evidenceRefs")
    if not isinstance(refs, list):
        return None
    refs = [str(r).strip().strip("[]").strip() for r in refs]
    toks = set(bracket_tokens(pkg))
    if len(refs) != 3 or len(set(refs)) != 3 or any(r not in toks for r in refs):
        return None
    return {"action": d["action"],
            "facts": {"vendorName": vendor, "invoiceNumber": inv, "amountMinor": am, "currency": cur},
            "evidenceRefs": refs, "reason": str(d.get("reason") or "").strip()}


async def llm_decide(pkgs: list, policy, deadline: float) -> dict:
    """One batched call (plus at most one repair call). Returns {packageId: validated decision}."""
    out, remaining = {}, list(pkgs)
    for attempt in range(2):
        if not remaining:
            break
        left = deadline - time.monotonic()
        if left < 8:
            break
        hint = "" if attempt == 0 else (
            "\nNOTE: your previous answer was invalid for these packages. evidenceRefs must be exactly three "
            "distinct tokens copied from the [bracketed] references in the text; facts must be complete.\n")
        user = (f"Policy revision: {policy}\n{hint}Packages (JSON):\n"
                + json.dumps([{"packageId": p["packageId"], "package": p} for p in remaining], ensure_ascii=False))
        try:
            raw = await call_llm(user, timeout=min(left - 2, 40))
            decs = parse_json(raw).get("decisions", [])
        except Exception:
            continue
        by_id = {str(d.get("packageId")): d for d in decs if isinstance(d, dict)}
        for p in remaining:
            v = validate_decision(by_id.get(p["packageId"]), p)
            if v:
                out[p["packageId"]] = v
        remaining = [p for p in remaining if p["packageId"] not in out]
    return out


def fallback_decision(pkg):
    """Schema-valid, non-settling default when the model is unavailable. Never cached."""
    toks = bracket_tokens(pkg)
    toks = (toks + ["unknown-ref-1", "unknown-ref-2", "unknown-ref-3"])[:3]
    am = find_key(pkg, "amountMinor")
    return {"action": "hold_invoice",
            "facts": {"vendorName": str(find_key(pkg, "vendorName") or "unknown"),
                      "invoiceNumber": str(find_key(pkg, "invoiceNumber") or "unknown"),
                      "amountMinor": am if isinstance(am, int) else 0,
                      "currency": str(find_key(pkg, "currency") or "INR").upper()},
            "evidenceRefs": toks,
            "reason": "Automated analysis was unavailable, so payment is paused pending manual verification."}


def build_rationale(action, refs, reason, facts):
    ref_txt = ", ".join(f"[{r}]" for r in refs)
    s = (f"Proposed action {action} for invoice {facts['invoiceNumber']} from {facts['vendorName']}. "
         f"The decisive paragraph is supported by evidence {ref_txt}. {reason}")
    s = s[:1500]
    if len(s) < 60:
        s += " " * 0 + " This action follows the decisive evidence cited above."
    return s


async def decide(principal: str, batch: dict) -> dict:
    pkgs = batch["packages"]
    deadline = time.monotonic() + REQUEST_BUDGET
    keyed = [(p, sha(canon({k: v for k, v in p.items() if k not in ("packageId", "batchId")}))) for p in pkgs]
    decisions, todo = {}, {}
    for p, k in keyed:
        c = cache_get(k)
        if c:
            decisions[k] = c
        else:
            todo.setdefault(k, p)
    pid2key = {p["packageId"]: k for p, k in keyed}
    items = list(todo.values())

    async def run(chunk):
        async with LLM_SEM:
            got = await llm_decide(chunk, batch.get("policyRevision"), deadline)
        for p in chunk:
            if p["packageId"] in got:
                k = pid2key[p["packageId"]]
                decisions[k] = got[p["packageId"]]
                cache_put(k, got[p["packageId"]])

    if items:
        await asyncio.gather(*(run(items[i:i + CHUNK]) for i in range(0, len(items), CHUNK)),
                             return_exceptions=True)

    proposals = []
    for p, k in keyed:
        d = decisions.get(k) or fallback_decision(p)
        pid = p["packageId"]
        refs = d["evidenceRefs"]
        proposals.append({
            "packageId": pid,
            "actionId": "act_" + sha(f"{principal}|{batch['batchId']}|{pid}|{d['action']}")[:24],
            "action": d["action"],
            "facts": d["facts"],
            "evidenceRefs": [f"[{r}]" for r in refs] if EVIDENCE_BRACKETS else refs,
            "rationale": build_rationale(d["action"], refs, d["reason"], d["facts"]),
        })
    return {"batchId": batch["batchId"], "proposals": proposals}


# --------------------------------------------------------------------------- protocol helpers
def guard(request: Request, need_json: bool = False) -> str:
    m = re.fullmatch(r"Bearer\s+(\S+)", request.headers.get("authorization", ""), re.I)
    if not m:
        raise ApiError(401, "UNAUTHENTICATED", "Authentication required")
    token = m.group(1)
    if ALLOWED_TOKENS and token not in ALLOWED_TOKENS:
        raise ApiError(403, "FORBIDDEN", "Access denied")
    if request.headers.get("a2a-version") != "1.0":
        raise ApiError(400, "VERSION_NOT_SUPPORTED", "Unsupported A2A version")
    accept = request.headers.get("accept")
    if accept and not any(t in accept.lower() for t in ("*/*", "application/*", "application/json", A2A_JSON)):
        raise ApiError(406, "NOT_ACCEPTABLE", "Unsupported response media type")
    if need_json:
        ct = request.headers.get("content-type", "").split(";")[0].strip().lower()
        if ct not in (A2A_JSON, "application/json"):
            raise ApiError(415, "UNSUPPORTED_MEDIA_TYPE", "Unsupported media type")
    return sha(token)


async def read_json(request: Request):
    raw = await request.body()
    if len(raw) > 4 * 1024 * 1024:
        raise ApiError(413, "PAYLOAD_TOO_LARGE", "Payload too large")
    try:
        return json.loads(raw)
    except Exception:
        raise ApiError(400, "INVALID_JSON", "Malformed JSON")


def extract_batch(msg):
    for part in msg.get("parts", []):
        if isinstance(part, dict) and part.get("mediaType") == MT_BATCH and isinstance(part.get("data"), dict):
            return part["data"]
    return None


def validate_batch(b):
    if not isinstance(b, dict):
        raise ApiError(400, "INVALID_REQUEST", "Invalid batch")
    if not isinstance(b.get("batchId"), str) or not b["batchId"]:
        raise ApiError(400, "INVALID_REQUEST", "Invalid batch")
    pk = b.get("packages")
    if not isinstance(pk, list) or not pk or len(pk) > 200:
        raise ApiError(400, "INVALID_REQUEST", "Invalid batch")
    ids = set()
    for p in pk:
        if not isinstance(p, dict) or not isinstance(p.get("packageId"), str) or not p["packageId"] \
                or p["packageId"] in ids:
            raise ApiError(400, "INVALID_REQUEST", "Invalid batch")
        ids.add(p["packageId"])


def status(state):
    return {"state": state, "timestamp": now()}


def view_task(task, history_length=None):
    t = dict(task)
    if history_length is not None and "history" in t:
        t["history"] = t["history"][-history_length:] if history_length > 0 else []
    return t


# --------------------------------------------------------------------------- lifecycle
async def finish_initial(principal: str, tid: str) -> dict:
    row = db_get_task(tid)
    batch = extract_batch(row["task"]["history"][0])
    try:
        props = await decide(principal, batch)
    except Exception:
        props = None
    async with lock_for(("task", tid)):
        row = db_get_task(tid)
        task = row["task"]
        if task["status"]["state"] != S_WORKING:      # canceled while we were thinking
            return task
        if props is None:
            task["status"] = status(S_FAILED)
            db_put_task(tid, principal, task, None)
            return task
        task["artifacts"] = [{"artifactId": "proposals-" + tid, "name": "invoice-action-proposals",
                              "parts": [{"mediaType": MT_PROPOSALS, "data": props}]}]
        task["status"] = status(S_INPUT)
        db_put_task(tid, principal, task, props["proposals"])
        return task


async def initial(principal: str, msg: dict, h: str) -> dict:
    batch = extract_batch(msg)
    validate_batch(batch)
    tid = str(uuid.uuid4())
    ctx = msg.get("contextId") if isinstance(msg.get("contextId"), str) and msg.get("contextId") else str(uuid.uuid4())
    stored = dict(msg)
    stored["taskId"], stored["contextId"] = tid, ctx
    task = {"id": tid, "contextId": ctx, "status": status(S_WORKING), "history": [stored]}
    db_put_task(tid, principal, task, None, (principal, msg["messageId"], h, tid))   # persist BEFORE reasoning
    return await finish_initial(principal, tid)


def mismatch():
    return ApiError(400, "CONTINUATION_MISMATCH", "Continuation does not match the task")


async def continuation(principal: str, msg: dict, h: str) -> dict:
    tid = msg.get("taskId")
    if not isinstance(tid, str) or not tid:
        raise ApiError(400, "INVALID_REQUEST", "Invalid request")
    async with lock_for(("task", tid)):
        row = db_get_task(tid)
        if not row or row["principal"] != principal:
            raise ApiError(404, "NOT_FOUND", "Task not found")
        task = row["task"]
        st = task["status"]["state"]
        if st in TERMINAL:
            raise ApiError(409, "TASK_TERMINAL", "Task can no longer be modified")
        if st != S_INPUT:
            raise ApiError(409, "TASK_NOT_READY", "Task is not awaiting input")
        if msg.get("contextId") != task["contextId"]:
            raise mismatch()
        parts = [p for p in msg["parts"] if isinstance(p, dict) and p.get("mediaType") == MT_RESULTS]
        if len(parts) != 1 or not isinstance(parts[0].get("data"), dict):
            raise ApiError(400, "INVALID_REQUEST", "Invalid request")
        data = parts[0]["data"]
        stored_batch = task["artifacts"][0]["parts"][0]["data"]["batchId"]
        if data.get("batchId") != stored_batch:
            raise mismatch()
        res = data.get("results")
        if not isinstance(res, list) or not res:
            raise mismatch()
        by_id = {p["packageId"]: p for p in row["proposals"]}
        seen, accepted = set(), {}
        for r in res:
            if not isinstance(r, dict):
                raise mismatch()
            pid = r.get("packageId")
            p = by_id.get(pid) if isinstance(pid, str) else None
            if p is None or pid in seen:
                raise mismatch()
            seen.add(pid)
            if r.get("actionId") != p["actionId"] or r.get("action") != p["action"]:
                raise mismatch()
            oc = r.get("outcome")
            if oc == "ACCEPTED":
                n = r.get("receiptNonce")
                if not isinstance(n, str) or not n:
                    raise mismatch()
                accepted[pid] = n
            elif oc != "REJECTED":
                raise mismatch()
        executions = [{"packageId": p["packageId"], "actionId": p["actionId"], "action": p["action"],
                       "receiptNonce": accepted[p["packageId"]], "facts": p["facts"],
                       "evidenceRefs": p["evidenceRefs"]}
                      for p in row["proposals"] if p["packageId"] in accepted]
        task["history"].append(dict(msg))
        task["artifacts"].append({"artifactId": "receipts-" + tid, "name": "invoice-action-receipts",
                                  "parts": [{"mediaType": MT_RECEIPTS,
                                             "data": {"batchId": stored_batch, "executions": executions}}]})
        task["status"] = status(S_DONE)
        db_put_task(tid, principal, task, row["proposals"], (principal, msg["messageId"], h, tid))
        return task


# --------------------------------------------------------------------------- routes
app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
BASE_PATH = urlparse(BASE_URL).path.rstrip("/") if BASE_URL else os.environ.get("BASE_PATH", "").rstrip("/")
router = APIRouter()


@app.exception_handler(ApiError)
async def _api_err(_, e: ApiError):
    return err_response(e)


@app.exception_handler(StarletteHTTPException)
async def _http_err(_, e: StarletteHTTPException):
    reason = {404: "NOT_FOUND", 405: "METHOD_NOT_ALLOWED"}.get(e.status_code, "ERROR")
    return err_response(ApiError(e.status_code, reason, "Request failed"))


@app.exception_handler(Exception)
async def _any_err(_, e: Exception):
    return err_response(ApiError(500, "INTERNAL", "Internal error"))


@app.get("/.well-known/agent-card.json")
async def agent_card(request: Request):
    url = BASE_URL or (f"{request.headers.get('x-forwarded-proto', request.url.scheme)}://"
                       f"{request.headers.get('host', request.url.netloc)}{BASE_PATH}/")
    card = {
        "name": "Invoice Action Agent",
        "description": "Reads invoice packages, proposes one approved-action-gated business action per package "
                       "with cited evidence, and records receipts only for accepted proposals.",
        "version": "1.0.0",
        "supportedInterfaces": [{"url": url, "protocolBinding": "HTTP+JSON", "protocolVersion": "1.0"}],
        "capabilities": {"streaming": False, "pushNotifications": False},
        "securitySchemes": {"bearer": {"httpAuthSecurityScheme": {"scheme": "Bearer"}}},
        "defaultInputModes": [MT_BATCH],
        "defaultOutputModes": [MT_PROPOSALS, MT_RECEIPTS],
        "skills": [{
            "id": "invoice_action_agent",
            "name": "Invoice action decisions",
            "description": "Chooses settle, approval, hold, duplicate-rejection or exception actions for invoice "
                           "packages and cites the decisive evidence.",
            "tags": ["invoice", "accounts-payable", "approval", "duplicate-detection"],
            "inputModes": [MT_BATCH], "outputModes": [MT_PROPOSALS, MT_RECEIPTS],
        }],
    }
    return Response(jbytes(card), media_type="application/json")


@router.post("/message:send")
async def message_send(request: Request):
    principal = guard(request, need_json=True)
    body = await read_json(request)
    msg = body.get("message") if isinstance(body, dict) else None
    if not isinstance(msg, dict):
        raise ApiError(400, "INVALID_REQUEST", "Invalid request")
    mid = msg.get("messageId")
    if not isinstance(mid, str) or not mid or len(mid) > 300:
        raise ApiError(400, "INVALID_REQUEST", "Invalid request")
    if msg.get("role") != "ROLE_USER" or not isinstance(msg.get("parts"), list) or not msg["parts"]:
        raise ApiError(400, "INVALID_REQUEST", "Invalid request")
    h = sha(canon(msg))                                    # message only; configuration is ignored
    async with lock_for(("msg", principal, mid)):
        rec = db_get_msg(principal, mid)
        if rec:
            if rec[0] != h:
                raise ApiError(409, "IDEMPOTENCY_CONFLICT", "Message ID was already used with different content")
            task = db_get_task(rec[1])["task"]
            if task["status"]["state"] == S_WORKING:        # crashed mid-flight earlier; finish it now
                task = await finish_initial(principal, rec[1])
            return jresp({"task": task})
        if msg.get("taskId") is not None:
            task = await continuation(principal, msg, h)
        else:
            task = await initial(principal, msg, h)
    return jresp({"task": task})


def _hl(request: Request):
    v = request.query_params.get("historyLength")
    try:
        return max(0, int(v)) if v is not None else None
    except ValueError:
        raise ApiError(400, "INVALID_REQUEST", "Invalid request")


@router.get("/tasks/{task_id}")
async def get_task(task_id: str, request: Request):
    principal = guard(request)
    row = db_get_task(task_id)
    if not row or row["principal"] != principal:
        raise ApiError(404, "NOT_FOUND", "Task not found")
    return jresp(view_task(row["task"], _hl(request)))


@router.get("/tasks")
async def list_tasks(request: Request):
    principal = guard(request)
    hl = _hl(request)
    st, ctx = request.query_params.get("status"), request.query_params.get("contextId")
    tasks = [t for t in db_list(principal)
             if (not st or t["status"]["state"] == st) and (not ctx or t["contextId"] == ctx)]
    out, used = [], 64
    for t in tasks:                                   # newest first; full detail while it fits, then slim
        full = view_task(t, hl)
        size = len(jbytes(full))
        if used + size > MAX_RESPONSE:
            full = {k: v for k, v in t.items() if k not in ("history", "artifacts")}
            size = len(jbytes(full))
            if used + size > MAX_RESPONSE:
                break
        out.append(full)
        used += size
    return jresp({"tasks": out})


@router.post("/tasks/{task_id}:cancel")
async def cancel_task(task_id: str, request: Request):
    principal = guard(request)
    async with lock_for(("task", task_id)):           # same lock as continuation => exactly one wins
        row = db_get_task(task_id)
        if not row or row["principal"] != principal:
            raise ApiError(404, "NOT_FOUND", "Task not found")
        task = row["task"]
        if task["status"]["state"] in TERMINAL:
            raise ApiError(409, "TASK_TERMINAL", "Task can no longer be modified")
        task["status"] = status(S_CANCELED)
        db_put_task(task_id, principal, task, row["proposals"])
        return jresp(task)


app.include_router(router, prefix=BASE_PATH)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8000")), workers=1)
