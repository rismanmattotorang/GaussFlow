# gaussflow-tui

**A terminal user interface for [GaussFlow](../README.md), by Gaussian Technologies.**

`gaussflow-tui` provides a keyboard-driven, in-terminal view for monitoring GaussFlow workflows
and executions — useful over SSH or in environments without a browser.

## Status 🟡 Partial

The TUI is a single-binary scaffold (~1.1k LOC). The layout and rendering loop are in place;
deeper integration with live engine state tracks the observability work in Phase 4 of the
[Production Roadmap](../docs/PRODUCTION_ROADMAP.md).

## Run

```bash
cargo run -p gaussflow-tui
```

Press `q` to quit (see in-app help for the full keymap).
</content>
