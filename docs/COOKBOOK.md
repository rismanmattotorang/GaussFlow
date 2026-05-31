# GaussFlow Cookbook

Runnable example workflows, one per capability. Each is a `WorkflowSpec` you can save to a file and
run with the CLI:

```bash
cargo run -p gaussflow-cli -- run example.json
# (runs with NO database by default and prints per-node outputs)
```

All examples use the offline `mock` model or pure compute nodes, so they run with **no API key**.
For live LLM calls, use a real `model` (`gpt-*`, `claude-*`, or `ollama/<model>`) and set the
matching key (`OPENAI_API_KEY` / `ANTHROPIC_API_KEY`).

---

## 1. LLM call (offline mock)

```json
{
  "name": "summarize",
  "nodes": [
    { "id": "ingest", "type": "data_processor", "params": { "op": "extract", "field": "text" } },
    { "id": "sum", "type": "llm_call", "params": { "model": "mock-sum", "prompt": "summarize" } }
  ],
  "connections": [ { "from": "ingest", "to": "sum", "on": "success" } ],
  "settings": { "concurrency": 2 }
}
```

## 2. Conditional branch (skips the untaken branch)

```json
{
  "name": "approve-or-reject",
  "nodes": [
    { "id": "check", "type": "conditional", "params": { "field": "score", "op": "gt", "value": 3 } },
    { "id": "approve", "type": "data_processor", "params": { "op": "set", "value": { "ok": true } } },
    { "id": "reject", "type": "data_processor", "params": { "op": "set", "value": { "ok": false } } }
  ],
  "connections": [
    { "from": "check", "to": "approve", "on": "true" },
    { "from": "check", "to": "reject", "on": "false" }
  ],
  "settings": {}
}
```
Run with input `{"score": 5}` → `approve` runs, `reject` is skipped.

## 3. Router (switch on a field)

```json
{
  "name": "route-by-tier",
  "nodes": [
    { "id": "router", "type": "router",
      "params": { "field": "tier", "routes": { "gold": "premium" }, "default": "standard" } },
    { "id": "premium", "type": "data_processor", "params": {} },
    { "id": "standard", "type": "data_processor", "params": {} }
  ],
  "connections": [
    { "from": "router", "to": "premium", "on": "premium" },
    { "from": "router", "to": "standard", "on": "standard" }
  ],
  "settings": {}
}
```

## 4. Ensemble (fan-in + aggregation)

```json
{
  "name": "ensemble-vote",
  "nodes": [
    { "id": "m1", "type": "data_processor", "params": { "op": "set", "value": { "label": "a" } } },
    { "id": "m2", "type": "data_processor", "params": { "op": "set", "value": { "label": "a" } } },
    { "id": "m3", "type": "data_processor", "params": { "op": "set", "value": { "label": "b" } } },
    { "id": "agg", "type": "ensemble", "params": { "strategy": "collect" } }
  ],
  "connections": [
    { "from": "m1", "to": "agg", "on": "success" },
    { "from": "m2", "to": "agg", "on": "success" },
    { "from": "m3", "to": "agg", "on": "success" }
  ],
  "settings": { "concurrency": 4 }
}
```

## 5. Agent (bounded tool loop, scripted offline)

```json
{
  "name": "agent-demo",
  "nodes": [
    { "id": "act", "type": "agent", "params": {
        "task": "shout the greeting",
        "max_steps": 3,
        "script": [ { "tool": "upper", "args": { "text": "hello" } }, { "final": "done" } ]
    } }
  ],
  "connections": [],
  "settings": {}
}
```
(Built-in tools: `echo`, `upper`, `sum`. Omit `script` to drive the loop with a real LLM via `model`.)

## 6. Parallel sub-workflows

```json
{
  "name": "parallel-demo",
  "nodes": [
    { "id": "fan", "type": "parallel", "params": { "branches": [
      { "name": "b1", "nodes": [ { "id": "x", "type": "data_processor", "params": { "op": "set", "value": { "b": 1 } } } ], "connections": [], "settings": {} },
      { "name": "b2", "nodes": [ { "id": "y", "type": "data_processor", "params": { "op": "set", "value": { "b": 2 } } } ], "connections": [], "settings": {} }
    ] } }
  ],
  "connections": [],
  "settings": {}
}
```

## 7. Subgraph (nested workflow)

```json
{
  "name": "subgraph-demo",
  "nodes": [
    { "id": "inner", "type": "subgraph", "params": { "workflow": {
      "name": "nested",
      "nodes": [ { "id": "n", "type": "data_processor", "params": { "op": "extract", "field": "text" } } ],
      "connections": [], "settings": {}
    } } }
  ],
  "connections": [],
  "settings": {}
}
```

---

## Synthesize instead of hand-writing

You usually shouldn't write these by hand — describe the goal and let GaussFlow compile it:

```bash
export OPENAI_API_KEY=sk-...        # or ANTHROPIC_API_KEY for a claude-* model
cargo run -p gaussflow-cli -- synth "Extract the text field, then summarize it" --run
```
