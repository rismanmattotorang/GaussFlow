# gaussflow-py

**Python bindings for [GaussFlow](../README.md), by Gaussian Technologies.**

`gaussflow-py` exposes the GaussFlow engine to Python via [PyO3](https://pyo3.rs/) and
[pyo3-asyncio](https://github.com/awestlake87/pyo3-asyncio), so you can validate and run
workflows from Python and `asyncio`.

## API

| Function | Description |
|---|---|
| `validate(workflow_json: str) -> (name, node_count, edge_count)` | Parse & validate a workflow; raises `ValueError` on failure. |
| `execute_py(workflow_json: str, input_json: str \| None) -> awaitable` | Execute a workflow; returns a coroutine resolving to the JSON result. |
| `__version__` | The crate/package version. |

## Example

```python
import asyncio, json
import gaussflow_py

spec = open("workflow.json").read()

print(gaussflow_py.validate(spec))          # -> ('hello-llm', 1, 0)

async def main():
    result = await gaussflow_py.execute_py(spec, json.dumps({}))
    print(result)

asyncio.run(main())
```

## Status 🟡 Partial

The bindings are thin and functional, but inherit the runtime's current constraints: execution
requires a live SurrealDB instance and returns a `run_id` rather than full outputs (see the
[runtime README](../gaussflow-runtime/README.md) and
[Production Roadmap](../docs/PRODUCTION_ROADMAP.md)).

## Build

Build with [maturin](https://github.com/PyO3/maturin):

```bash
pip install maturin
maturin develop -m gaussflow-py/Cargo.toml
```
</content>
