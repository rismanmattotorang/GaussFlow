//! The capability catalog: the node types the runtime can actually execute today.
//!
//! Synthesis is only allowed to emit these capabilities — this is the "capability honesty" rule
//! from the design (`docs/SYNTHESIS_PIPELINE.md`): the compiler must never produce a node type the
//! runtime only stubs. The list is kept in lockstep with `gaussflow_runtime::handler::handler_for`.

/// Node types the runtime executes. Capability strings equal the `NodeType` JSON names.
pub const SUPPORTED_CAPABILITIES: &[&str] = &[
    "llm_call",
    "agent",
    "ensemble",
    "router",
    "subgraph",
    "data_processor",
    "conditional",
    "parallel",
];

/// A human/LLM-readable description of each capability and its key params, embedded in the
/// planning prompt so the model only proposes things the runtime can run.
pub fn catalog_description() -> String {
    [
        "- data_processor: deterministic transform. params: op (passthrough|extract|set), field, value",
        "- llm_call: call a language model. params: model, prompt",
        "- conditional: compare an input field. params: field, op (eq|ne|gt|lt|ge|le), value. Emits a true/false branch; label outgoing edges \"true\"/\"false\".",
        "- router: switch on an input field. params: field, routes (map value->label), default. Label outgoing edges with the route names.",
        "- ensemble: aggregate multiple predecessors. params: strategy (collect|first|vote), field",
        "- agent: bounded tool-use loop. params: task, max_steps, model",
        "- subgraph: run a nested workflow. params: workflow (an inline workflow spec)",
        "- parallel: run inline branches concurrently. params: branches (array of workflow specs)",
    ]
    .join("\n")
}

/// Whether `capability` is one the runtime supports.
pub fn is_supported(capability: &str) -> bool {
    SUPPORTED_CAPABILITIES.contains(&capability)
}
