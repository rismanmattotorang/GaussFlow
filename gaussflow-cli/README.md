# gaussflow-cli

**The `gaussflow` command-line interface, by Gaussian Technologies.**

`gaussflow-cli` is the primary way to interact with [GaussFlow](../README.md) from a terminal:
validate workflow specs, run them, manage templates and history, and start the server.

## Commands

| Command | Purpose | Status |
|---|---|---|
| `validate` | Validate a workflow specification | 🟡 |
| `run` | Execute a workflow | 🟡 |
| `template` | Manage workflow templates/libraries | 🟡 |
| `monitor` | Monitor running workflows | 🟡 |
| `history` | Inspect run history and artifacts | 🟡 |
| `config` | Configure GaussFlow | 🟡 |
| `server` | Start the GaussFlow server | 🟡 |
| `benchmark` | Performance testing | 🟡 |
| `database` | Database management | 🟡 |
| `cache` | Cache management | 🟡 |

> **Status:** the command tree, configuration manager, cache manager, and database manager are
> scaffolded with `clap`, but wiring to the canonical runtime is incomplete and some commands
> are placeholders. See the [Production Roadmap](../docs/PRODUCTION_ROADMAP.md).

> **⚠️ Security note:** the bundled default config currently contains hardcoded credentials and
> a default JWT secret. These are scheduled for removal in Phase 0 of the roadmap. Do not use
> the defaults in any real deployment.

## Usage

```bash
cargo run -p gaussflow-cli -- --help
cargo run -p gaussflow-cli -- validate workflow.json
OPENAI_API_KEY=sk-... cargo run -p gaussflow-cli -- run workflow.json
```

Global flags: `--verbose`, `--debug`, `--config <path>`, `--profile`, `--metrics`.
</content>
