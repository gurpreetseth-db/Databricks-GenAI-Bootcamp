# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Step 1 - Install Stateful Agents SDK
# MAGIC %pip install --upgrade "databricks-agentbricks>=0.6.0" -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Step 1b - Reference Parameters
# MAGIC %run ./Config_Parameters

# COMMAND ----------

# DBTITLE 1,Module 13 — Welcome
# MAGIC %md
# MAGIC ## 🧠 DataBank AI Lab — Module 13: Stateful Agents (Sessions, Memory & Memory Pipeline)
# MAGIC **Duration:** ~35 minutes | **Prerequisites:** Module 07 completed (Supervisor Agent endpoint is READY)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### What are Stateful Agents?
# MAGIC
# MAGIC Databricks now provides **managed state** for agents, backed by **Lakebase**:
# MAGIC
# MAGIC | Building block | Scope | What it stores | DataBank use |
# MAGIC |----------------|-------|----------------|--------------|
# MAGIC | **Agent Session** | One conversation | Ordered transcript: messages, tool calls, results | "Which customer were we just discussing?" |
# MAGIC | **Agent Memory** | Across conversations | Durable facts & preferences, found by **semantic search** | "This advisor's client prefers low-risk products" |
# MAGIC | **Memory Pipeline** | Sessions → Memory | An LLM *distills* sessions into memory (Distill → Merge → Maintain), on demand or ~every 24h | Turns yesterday's chats into today's recall |
# MAGIC
# MAGIC > Deleting a session does **not** delete memory already distilled from it. The two stores are independent.
# MAGIC
# MAGIC ### What we build
# MAGIC
# MAGIC 1. **Part A — A brand-new session:** raw Session + Memory APIs, no agent involved.
# MAGIC 2. **Part B — The existing Module 07 agent:** its endpoint can't be modified, so we add state with a thin **memory wrapper** (recall → inject → call agent → record).
# MAGIC 3. **Part C — Memory Pipeline:** distill sessions into long-term memory.
# MAGIC 4. **Part D — A new session that remembers:** same advisor, new conversation, no history — the agent still knows their preferences.
# MAGIC
# MAGIC ### How is this different from `databank-chat-demo`?
# MAGIC The chat app (Module 12) uses a **custom** Lakebase memory manager. This module uses the **platform-managed** Sessions / Memory / Pipeline APIs, so there is no schema or fact-extraction code for you to maintain.

# COMMAND ----------

# DBTITLE 1,Step 2 — Stateful Agent Configuration
import json, os, re, time, uuid
import requests
from databricks.sdk import WorkspaceClient
from databricks_agentkit import AgentKitClient

w = WorkspaceClient()
client = AgentKitClient(w)

# Store IDs: 3-56 chars, lowercase alphanumeric + hyphens. They CANNOT be changed after creation.
SESSION_STORE_ID = f"{username_clean.lower()}-databank-sessions"
MEMORY_STORE_ID  = f"{username_clean.lower()}-databank-memory"

# actor_id identifies WHOSE state this is. In production take it from your verified
# identity layer (e.g. the signed-in user) — NEVER from untrusted request input.
ACTOR_ID = f"advisor-{username_clean.lower()}"

PIPELINE_MODEL = "system.ai.claude-sonnet-5"

PIPELINE_INSTRUCTIONS = (
    "Remember durable facts about the financial advisor's clients and working preferences: "
    "stated risk appetite, investment goals, product preferences and dislikes, communication "
    "preferences, and corrections the advisor made. "
    "Do NOT store account numbers, balances, card numbers, passwords, or other credentials."
)

print("=" * 60)
print(" STATEFUL AGENT CONFIGURATION ")
print("=" * 60)
print(f"🗂️  Session store : {SESSION_STORE_ID}")
print(f"🧠 Memory store  : {MEMORY_STORE_ID}")
print(f"👤 Actor         : {ACTOR_ID}")
print(f"🤖 Pipeline model: {PIPELINE_MODEL}")

# COMMAND ----------

# DBTITLE 1,Step 3 — Create Session Store & Memory Store (idempotent)
def get_or_create(accessor, store_id, description):
    """Return the store if it exists, otherwise create it."""
    try:
        store = accessor.get(store_id)
        print(f"✅ '{store_id}' already exists — reusing")
        return store
    except Exception:
        store = accessor.create(store_id, description=description)
        print(f"✅ Created '{store_id}'")
        return store

session_store = get_or_create(
    client.session_stores, SESSION_STORE_ID, "DataBank advisor conversations"
)
memory_store = get_or_create(
    client.memory_stores, MEMORY_STORE_ID, "DataBank advisor long-term memory"
)

# COMMAND ----------

# DBTITLE 1,Part A — Sessions: a brand-new conversation
# MAGIC %md
# MAGIC ## Part A — A brand-new agent session
# MAGIC
# MAGIC A **session** is keyed by `actor_id` (who owns it) and an optional caller-chosen `session_id`.
# MAGIC Items are **immutable** and returned with auto-assigned `item_id` / `create_time`.

# COMMAND ----------

# DBTITLE 1,Part A.1 — Create a session and append items
demo_session_id = f"demo-{uuid.uuid4().hex[:8]}"

session = session_store.add(actor_id=ACTOR_ID, session_id=demo_session_id)

session.append_items([
    {"type": "message", "role": "user",
     "content": "I'm meeting a cautious client tomorrow. They hate anything volatile."},
    {"type": "message", "role": "assistant",
     "content": "Understood — I'll focus on low-volatility products such as savings and bonds."},
])

print(f"✅ Session '{demo_session_id}' created with 2 items")

# COMMAND ----------

# DBTITLE 1,Part A.2 — Resume the session later and read it back
resumed = session_store.get(demo_session_id)
history = [item.data for item in resumed.list_items(order_by="create_time asc")]

print(f"📜 {len(history)} items in session '{demo_session_id}':")
for item in history:
    print(f"   [{item.get('role')}] {item.get('content')}")

# Handy session operations:
#   resumed.pop()    -> remove the last item (e.g. retry a bad turn)
#   resumed.clear()  -> empty the session
# Child sessions for sub-agents: session_store.add(actor_id=..., parent_session_id=demo_session_id)

# COMMAND ----------

# DBTITLE 1,Part A.3 — Agent Memory: write and search
# MAGIC %md
# MAGIC ### Agent Memory
# MAGIC
# MAGIC Memory entries are identified by `actor_id` + `path` (and an optional `session_id`).
# MAGIC Omit `session_id` for **cross-session** memory. `description` helps retrieval ranking.
# MAGIC
# MAGIC | Call | Behaviour |
# MAGIC |------|-----------|
# MAGIC | `add(...)` | Create — fails if the path already exists |
# MAGIC | `put(...)` | Create **or update** |
# MAGIC | `list(actor_id, path=...)` | Exact lookup / listing |
# MAGIC | `search(actor_id, query, limit)` | **Semantic** search (max 100 results, no pagination) |

# COMMAND ----------

# DBTITLE 1,Part A.4 — Memory CRUD
memory_store.put(
    actor_id=ACTOR_ID,
    path="/preferences/communication.md",
    content="Prefers concise bullet-point answers. Works in the London office (GMT).",
    description="Advisor communication preferences",
)
memory_store.put(
    actor_id=ACTOR_ID,
    path="/clients/risk-appetite.md",
    content="Client Mrs. Patel is a conservative investor who dislikes volatile products.",
    description="Client risk appetite",
)

def entry_text(entry):
    """Best-effort text of a memory entry (attribute names differ slightly between SDK releases)."""
    for attr in ("content", "text", "data"):
        value = getattr(entry, attr, None)
        if value:
            return str(value)
    return str(entry)

print("🔎 Semantic search: 'how does the advisor like answers formatted?'")
for hit in memory_store.search(actor_id=ACTOR_ID, query="how does the advisor like answers formatted?", limit=3):
    print(f"   • {getattr(hit, 'path', '?')}: {entry_text(hit)}")

# COMMAND ----------

# DBTITLE 1,Part B — Adding state to the existing Module 07 agent
# MAGIC %md
# MAGIC ## Part B — Adding state to the existing Module 07 agent
# MAGIC
# MAGIC The Module 07 agent is an **AgentBricks Supervisor Agent endpoint**. You can't edit its server code, so we
# MAGIC wrap it. For every turn the wrapper:
# MAGIC
# MAGIC ```
# MAGIC   user message ─▶ 1. load session history          (Session store)
# MAGIC                    2. search long-term memory        (Memory store)
# MAGIC                    3. call the unchanged agent with history + recalled facts
# MAGIC                    4. append user + assistant turn   (Session store)
# MAGIC ```
# MAGIC
# MAGIC > For agents whose **code you own** (OpenAI Agents SDK / LangGraph) use the built-in integrations instead:
# MAGIC > `databricks_agentkit.openai.session_store` + `memory_tools`, or `databricks_agentkit.langgraph.checkpointer` + `memory_tools`.

# COMMAND ----------

# DBTITLE 1,Part B.1 — Resolve the Module 07 endpoint
def resolve_agent_endpoint():
    """Find the Supervisor Agent serving endpoint created in Module 07."""
    all_eps = list(w.serving_endpoints.list())
    names = [ep.name for ep in all_eps]

    if "ENDPOINTNAME" in globals() and ENDPOINTNAME in names:   # set by Module 07 if run in the same session
        return ENDPOINTNAME
    if AGENT_ENDPOINT in names:
        return AGENT_ENDPOINT

    normalise = lambda s: re.sub(r"[^a-z0-9]", "", s.lower())
    term = normalise(f"{username_clean}_DataBank-AI-Advisor")
    match = next((n for n in names if term in normalise(n)), None)
    if match:
        return match

    me = w.current_user.me().user_name
    mine = [ep.name for ep in all_eps
            if ep.name.startswith("mas-") and ep.name.endswith("-endpoint") and ep.creator == me]
    return mine[0] if mine else None

AGENT_ENDPOINT_NAME = resolve_agent_endpoint()
assert AGENT_ENDPOINT_NAME, "❌ Could not find the Module 07 endpoint — finish Module 07 first."
print(f"🤖 Using agent endpoint: {AGENT_ENDPOINT_NAME}")

# COMMAND ----------

# DBTITLE 1,Part B.2 — The StatefulAdvisor wrapper
def _auth_headers():
    token = w.config.authenticate().get("Authorization", "").replace("Bearer ", "")
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def call_agent(messages, timeout=300):
    """Call the (unchanged) Module 07 endpoint. AgentBricks expects `input`, not `messages`."""
    resp = requests.post(
        f"{w.config.host}/serving-endpoints/{AGENT_ENDPOINT_NAME}/invocations",
        headers=_auth_headers(), json={"input": messages}, timeout=timeout,
    )
    resp.raise_for_status()
    data = resp.json()

    # Final answer = last assistant 'message' event containing an 'output_text' block (same parsing as Module 07)
    for event in reversed(data.get("output", []) if isinstance(data.get("output"), list) else []):
        if event.get("type") == "message" and event.get("role") == "assistant":
            for block in event.get("content", []):
                if block.get("type") == "output_text" and block.get("text"):
                    return block["text"].strip()
    return data.get("choices", [{}])[0].get("message", {}).get("content") or str(data)


class StatefulAdvisor:
    """Adds Agent Sessions + Agent Memory to a stateless AgentBricks endpoint."""

    def __init__(self, actor_id, session_id, use_session=True, use_memory=True, recall_limit=5):
        self.actor_id, self.session_id = actor_id, session_id
        self.use_session, self.use_memory, self.recall_limit = use_session, use_memory, recall_limit
        self.session = session_store.add(actor_id=actor_id, session_id=session_id) if use_session else None

    def _history(self):
        if not self.session:
            return []
        items = [i.data for i in self.session.list_items(order_by="create_time asc")]
        return [{"role": i["role"], "content": i["content"]} for i in items if i.get("type") == "message"]

    def _recall(self, query):
        if not self.use_memory:
            return []
        hits = memory_store.search(actor_id=self.actor_id, query=query, limit=self.recall_limit)
        return [entry_text(h) for h in hits]

    def ask(self, message):
        facts = self._recall(message)
        enriched = message
        if facts:
            # Injected into the user turn (not a system role) so any endpoint accepts it
            enriched = ("[Remembered context about this advisor and their clients — use it if relevant]\n"
                        + "\n".join(f"- {f}" for f in facts)
                        + f"\n\n[Question]\n{message}")
        answer = call_agent(self._history() + [{"role": "user", "content": enriched}])
        if self.session:
            # Store the ORIGINAL message, not the enriched one, so memory context isn't replayed as history
            self.session.append_items([
                {"type": "message", "role": "user", "content": message},
                {"type": "message", "role": "assistant", "content": answer},
            ])
        return answer

print("✅ StatefulAdvisor ready")

# COMMAND ----------

# DBTITLE 1,Part B.3 — Baseline: the stateless agent forgets
q1 = "I'm advising a client, Mrs. Patel, who is very risk-averse and wants capital preservation. Noted?"
q2 = "Which of the DataBank savings or investment products would you suggest for her?"

print("🧪 BASELINE — stateless agent, two separate calls")
print(f"\n👤 {q1}\n🤖 {call_agent([{'role': 'user', 'content': q1}])[:400]}")
print(f"\n👤 {q2}\n🤖 {call_agent([{'role': 'user', 'content': q2}])[:400]}")
print("\n⚠️  The second answer has no idea who 'her' is.")

# COMMAND ----------

# DBTITLE 1,Part B.4 — Session memory: the agent follows the conversation
chat_session_id = f"chat-{uuid.uuid4().hex[:8]}"
advisor = StatefulAdvisor(ACTOR_ID, chat_session_id, use_memory=False)   # session only

print(f"🧪 SESSION MEMORY ONLY — session '{chat_session_id}'")
print(f"\n👤 {q1}\n🤖 {advisor.ask(q1)[:400]}")
print(f"\n👤 {q2}\n🤖 {advisor.ask(q2)[:600]}")
print("\n✅ The follow-up is understood because the session carried the earlier turn.")

# COMMAND ----------

# DBTITLE 1,Part C — Memory pipeline
# MAGIC %md
# MAGIC ## Part C — Memory pipeline: sessions → long-term memory
# MAGIC
# MAGIC A **memory pipeline** reads a session store, has an LLM extract durable facts, and writes them to a memory store:
# MAGIC
# MAGIC | Stage | What happens |
# MAGIC |-------|--------------|
# MAGIC | **Distill** | Extract facts from conversations |
# MAGIC | **Merge** | Deduplicate / consolidate with existing memory |
# MAGIC | **Maintain** | Reorganise entries |
# MAGIC
# MAGIC Triggers: `MANUAL_ONLY` (default) or `SCHEDULED` (~every 24h, interval is fixed).
# MAGIC The `agentbricks` CLI isn't available on serverless, so we call the REST API (`/api/2.0/agents/memory-pipelines`) through the SDK.

# COMMAND ----------

# DBTITLE 1,Part C.1 — Create the pipeline (idempotent)
PIPELINES_API = "/api/2.0/agents/memory-pipelines"

def find_existing_pipeline():
    resp = w.api_client.do("GET", PIPELINES_API)
    for p in resp.get("memory_pipelines", resp.get("pipelines", [])):
        if p.get("session_store", "").endswith(SESSION_STORE_ID) and p.get("memory_store", "").endswith(MEMORY_STORE_ID):
            return p
    return None

try:
    pipeline = find_existing_pipeline()
    if pipeline:
        print("✅ Pipeline already exists — reusing")
    else:
        pipeline = w.api_client.do("POST", PIPELINES_API, body={
            "session_store": SESSION_STORE_ID,
            "memory_store": MEMORY_STORE_ID,
            "dreamer_policy": {
                "model": PIPELINE_MODEL,
                "instructions": PIPELINE_INSTRUCTIONS,
                "trigger": "MANUAL_ONLY",
            },
        })
        print("✅ Pipeline created")
    print(json.dumps(pipeline, indent=2, default=str))
except Exception as e:
    raise RuntimeError(f"❌ Could not create/list the memory pipeline: {e}") from e

# Resource names look like 'memory-pipelines/<id>' — the REST path wants just the id
PIPELINE_ID = (pipeline.get("name") or pipeline.get("id") or "").split("/")[-1]
assert PIPELINE_ID, f"Could not read the pipeline id from: {pipeline}"
print(f"🆔 Pipeline id: {PIPELINE_ID}")

# COMMAND ----------

# DBTITLE 1,Part C.2 — Give the pipeline something to learn from
# Simulate a day of work: a few short conversations with preferences and corrections buried in them.
seed_conversations = [
    [("user", "Mrs. Patel is retiring in 2 years and absolutely cannot stomach market swings."),
     ("assistant", "Noted — capital preservation is the priority for Mrs. Patel.")],
    [("user", "Please always give me answers as short bullet points, I read them on my phone between meetings."),
     ("assistant", "Will do — concise bullets from now on.")],
    [("user", "Actually, Mr. Okafor wants ethical / ESG-only funds. Ignore what I said about him earlier."),
     ("assistant", "Understood — Mr. Okafor: ESG-only funds.")],
]
for turns in seed_conversations:
    s = session_store.add(actor_id=ACTOR_ID, session_id=f"seed-{uuid.uuid4().hex[:8]}")
    s.append_items([{"type": "message", "role": r, "content": c} for r, c in turns])
print(f"✅ Seeded {len(seed_conversations)} conversations (plus the sessions from Part A & B)")

# COMMAND ----------

# DBTITLE 1,Part C.3 — Run the pipeline and wait
run = w.api_client.do("POST", f"{PIPELINES_API}/{PIPELINE_ID}/run", body={})
RUN_ID = (run.get("name") or run.get("id") or "").split("/")[-1]
print(f"▶️  Run started: {run.get('name', RUN_ID)}  state={run.get('state')}")

deadline = time.time() + 600
state = run.get("state")
while time.time() < deadline:
    status = w.api_client.do("GET", f"{PIPELINES_API}/{PIPELINE_ID}/runs/{RUN_ID}")
    state = status.get("state")
    print(f"   ⏳ {state}")
    if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
        break
    time.sleep(15)

if state == "SUCCEEDED":
    print("✅ Memory pipeline run SUCCEEDED")
else:
    print(f"⚠️  Run ended in state '{state}'. Details: {json.dumps(status, indent=2, default=str)}")
    print("   Only one run can be in progress at a time; re-run this cell once it finishes.")

# COMMAND ----------

# DBTITLE 1,Part C.4 — Inspect what the pipeline remembered
print(f"🧠 Memory entries for {ACTOR_ID}:\n")
for entry in memory_store.list(actor_id=ACTOR_ID):
    print(f"📄 {getattr(entry, 'path', '?')}")
    print(f"   {entry_text(entry)[:300]}\n")

print("🔎 Search: 'what does Mr. Okafor want?'")
for hit in memory_store.search(actor_id=ACTOR_ID, query="what does Mr. Okafor want?", limit=3):
    print(f"   • {getattr(hit, 'path', '?')}: {entry_text(hit)[:200]}")

# COMMAND ----------

# DBTITLE 1,Part D — A new session that remembers
# MAGIC %md
# MAGIC ## Part D — A brand-new session that remembers
# MAGIC
# MAGIC New `session_id`, **empty** history, same `actor_id`. The only thing carrying knowledge over is **Agent Memory**.

# COMMAND ----------

# DBTITLE 1,Part D.1 — Fresh session, long-term recall
new_session_id = f"chat-{uuid.uuid4().hex[:8]}"
fresh = StatefulAdvisor(ACTOR_ID, new_session_id, use_session=True, use_memory=True)

print(f"🆕 New session '{new_session_id}' — history items: {len(fresh._history())}\n")

for question in [
    "I'm seeing Mr. Okafor shortly. What kind of funds should I bring up?",
    "And how should you format your answers for me?",
]:
    print(f"👤 {question}")
    print(f"🤖 {fresh.ask(question)[:600]}\n")

# COMMAND ----------

# DBTITLE 1,Governance & gotchas
# MAGIC %md
# MAGIC ## 🔐 Governance & Gotchas
# MAGIC
# MAGIC | Topic | What to know |
# MAGIC |-------|--------------|
# MAGIC | **Access control** | Enforced at the **store** level, not per entry — anyone with access can read every entry. Partition by `actor_id` (per-user, per-team, or composite) and use separate stores for different sensitivity levels |
# MAGIC | **`actor_id`** | Must come from a trusted source (verified identity), never raw client input |
# MAGIC | **Immutability** | Store IDs cannot be renamed; pipeline session/memory stores are fixed at creation |
# MAGIC | **Pipeline limits** | One run at a time · ~1,200 UUID-style actors per run · scheduled interval fixed at ~24h |
# MAGIC | **What pipelines skip** | Dynamic values, schema details and credentials — keep PII/secrets out via `instructions` |
# MAGIC | **Search** | Up to 100 results, no pagination |
# MAGIC | **Same-session calls** | Run sequentially by design |
# MAGIC | **Cost** | During preview you pay only for the underlying Lakebase instance |
# MAGIC | **Legacy** | The earlier Unity Catalog–governed managed memory is superseded by memory stores |
# MAGIC
# MAGIC ### Which pattern when?
# MAGIC
# MAGIC | Your agent | Approach |
# MAGIC |------------|----------|
# MAGIC | Code you own (OpenAI Agents SDK / LangGraph) | Built-in `databricks_agentkit.openai` / `.langgraph` integrations, deployed with `agentbricks deploy` |
# MAGIC | Managed endpoint you can't edit (Module 07 Supervisor Agent) | Wrapper pattern from Part B |

# COMMAND ----------

# DBTITLE 1,Optional cleanup
# Uncomment to remove the demo state. Deleting sessions does NOT delete memory already distilled from them.
# w.api_client.do("DELETE", f"{PIPELINES_API}/{PIPELINE_ID}")
# for e in memory_store.list(actor_id=ACTOR_ID):
#     e.delete()
# for sid in [demo_session_id, chat_session_id, new_session_id]:
#     session_store.get(sid).clear()
print("ℹ️  Cleanup is commented out — uncomment the lines above to reset this module.")

# COMMAND ----------

# DBTITLE 1,Module 13 — Checkpoint
# MAGIC %md
# MAGIC ## ✅ Module 13 Complete — Checkpoint
# MAGIC
# MAGIC | Check | Expected |
# MAGIC |-------|----------|
# MAGIC | Session store + memory store | Created (`<username>-databank-sessions` / `-memory`) |
# MAGIC | Part A | Session items read back in order; memory `search` returns the right entry |
# MAGIC | Part B | Stateless baseline loses "her"; session-backed agent keeps it |
# MAGIC | Part C | Pipeline run `SUCCEEDED`; distilled entries visible with `memory_store.list` |
# MAGIC | Part D | New session answers using Mr. Okafor's ESG preference and the bullet-point format |
# MAGIC
# MAGIC **Key variables:** `SESSION_STORE_ID`, `MEMORY_STORE_ID`, `PIPELINE_ID`
# MAGIC
# MAGIC **Next:** wire the same stores into the chat app, or re-run Modules 09–10 to evaluate answers with vs. without memory.
