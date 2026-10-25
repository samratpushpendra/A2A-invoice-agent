# A2A Invoice Agent

Public HTTPS A2A 1.0 (HTTP+JSON) agent: reads invoice batches, proposes one action per package with cited evidence,
and records receipts only for proposals the grader accepts.

## Run

```bash
pip install fastapi "uvicorn[standard]" httpx

export BASE_URL="https://YOUR-HOST/a2a/"     # EXACT string you submit to the grader
export LLM_PROVIDER=openai                   # openai (any OpenAI-compatible API) | anthropic | mock
export LLM_BASE_URL=https://api.openai.com/v1
export LLM_API_KEY=sk-...
export LLM_MODEL=gpt-4o-mini
uvicorn app:app --host 0.0.0.0 --port 8000 --workers 1     # ONE worker: locks are in-process
```

Cheap / free model options (all OpenAI-compatible, just change the three LLM_* vars):

| Provider | LLM_BASE_URL | Example model |
|---|---|---|
| Google Gemini | `https://generativelanguage.googleapis.com/v1beta/openai` | `gemini-2.0-flash` |
| Groq | `https://api.groq.com/openai/v1` | `llama-3.3-70b-versatile` |
| OpenRouter | `https://openrouter.ai/api/v1` | any `:free` model |
| Local Ollama | `http://localhost:11434/v1` | `qwen2.5:14b` (key can be anything) |
| Anthropic | set `LLM_PROVIDER=anthropic` | a Haiku-class model |

If a provider rejects `response_format`, set `LLM_JSON_MODE=0`.

## Make it public over HTTPS
Deploy anywhere that gives you a stable HTTPS URL (Render, Fly.io, Railway, a VPS behind Caddy), or for quick tests
`cloudflared tunnel --url http://localhost:8000`. If you use `/a2a/` as the path, set `BASE_URL` to that exact URL; the
app mounts all routes under its path. Do not add redirects. Keep `DB_PATH` on a persistent disk.

## Test the protocol locally (no API key needed)

```bash
LLM_PROVIDER=mock BASE_URL=http://localhost:8000/a2a/ uvicorn app:app --workers 1 &
BASE=http://localhost:8000/a2a python selftest.py
```

## Design notes
- **Auth**: any non-empty Bearer token is a separate principal (SHA-256 of the token is stored, never the token).
  Missing -> 401. Set `ALLOWED_TOKENS=a,b` only if you want to restrict tokens.
- **Idempotency**: key `(principal, messageId)`, hash of recursively key-sorted compact JSON of `message` only.
  Per-key async lock means concurrent twins wait and get the same Task; changed content -> 409 `IDEMPOTENCY_CONFLICT`.
- **Persistence before reasoning**: the Task (WORKING) and idempotency record are committed before the model runs.
- **Decision cache**: key = SHA-256 of canonical package JSON (minus `packageId`/`batchId`), stored in SQLite, so
  Checks/Save re-use the core 60 packages with zero model calls; only new audit packages hit the model (12 per call).
  Polls, lists, replays, conflicts, cancels and continuations never call the model.
- **Action gate**: continuation must match principal, task, context, batch, package, actionId and action, otherwise
  400 and no state change. Receipts contain ACCEPTED results only, copied from the *stored* proposal.
- **Race**: continuation and cancel share one per-task lock, so exactly one returns 200 and the other 409.
  Terminal tasks are never written again.
- **Errors** are generic (no task IDs echoed; unknown and not-yours are indistinguishable).
- **List size**: newest tasks come back in full until ~500 KiB, older ones are slimmed (no history/artifacts).
- Unavailable/invalid model output falls back to a schema-valid `hold_invoice` (never `settle_invoice`) and is not cached.

## Tuning knobs if feedback shows a failure code
- Evidence wrong everywhere -> try `EVIDENCE_BRACKETS=1` (refs returned as `[ref]` instead of `ref`).
- Receipt artifact location: receipts are a second artifact (proposal artifact stays byte-identical). If the grader
  wants it as a second Part of the same artifact, change the `task["artifacts"].append(...)` in `continuation()`
  to `task["artifacts"][0]["parts"].append(...)`.
- Decision quality low -> use a stronger model for the first Check (results are cached afterwards).

Budget your 10 checks: 1) run with a good model, 2) read feedback, 3) fix, ... and **keep the last one for Save**.
