use crate::provider::{provider_for, CompletionRequest};
use async_trait::async_trait;
use gaussflow_core::model::{NodeConfig, NodeSpec, NodeType};
use serde_json::{json, Value};
use std::collections::BTreeMap;

/// Input delivered to a node handler.
///
/// `merged` is a shallow merge of all taken predecessors' outputs (or the run input for source
/// nodes) — convenient for the common single-input case. `sources` carries each taken
/// predecessor's `(node_id, output)` individually ("named ports"), which fan-in nodes such as
/// `ensemble` need because the merge is lossy when predecessors share output keys.
#[derive(Debug, Clone, Default)]
pub struct NodeInput {
    /// Shallow merge of upstream outputs (or the run input for source nodes).
    pub merged: Value,
    /// Each taken predecessor's `(node_id, output)`, in edge order.
    pub sources: Vec<(String, Value)>,
}

#[async_trait]
pub trait NodeHandler: Send + Sync {
    async fn execute(
        &self,
        node: &NodeSpec,
        input: NodeInput,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>>;
}

#[derive(Debug)]
pub struct LlmCallHandler;
#[async_trait]
impl NodeHandler for LlmCallHandler {
    async fn execute(
        &self,
        node: &NodeSpec,
        _input: NodeInput,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        // Resolve the model from (in order): typed config, a generic config map, or `params`.
        // The `params` fallback lets a uniform `{type, params}` node shape (as produced by the
        // synthesis layer) work without special-casing llm_call.
        let params_model = node.params.get("model").and_then(|m| m.as_str());
        let model = match &node.config {
            NodeConfig::LlmCall { model, .. } => model.as_str(),
            NodeConfig::Generic(map) => map
                .get("model")
                .and_then(|m| m.as_str())
                .or(params_model)
                .unwrap_or("gpt-3.5-turbo"),
            _ => params_model.unwrap_or("gpt-3.5-turbo"),
        };

        // Get temperature from config or params
        let temperature = match &node.config {
            NodeConfig::LlmCall { temperature, .. } => *temperature,
            _ => node
                .params
                .get("temperature")
                .and_then(|t| t.as_f64())
                .map(|t| t as f32),
        };

        let prompt = node
            .params
            .get("prompt")
            .and_then(|p| p.as_str())
            .unwrap_or("Hello")
            .to_string();

        // Dispatch through the provider abstraction (OpenAI, mock, ...).
        let provider = provider_for(model);
        let req = CompletionRequest {
            model: model.to_string(),
            prompt,
            system: None,
            temperature,
        };
        let answer = provider.complete(&req).await?;

        Ok(json!({
            "llm_call": node.id,
            "model": model,
            "provider": provider.name(),
            "answer": answer,
        }))
    }
}

/// Built-in, deterministic agent tools (no network). Returns the tool's result value.
fn run_tool(name: &str, args: &Value) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    match name {
        // Echo the args straight back.
        "echo" => Ok(args.clone()),
        // Uppercase `args.text`.
        "upper" => {
            let text = args.get("text").and_then(|v| v.as_str()).unwrap_or("");
            Ok(json!({ "text": text.to_uppercase() }))
        }
        // Sum the numbers in `args.values`.
        "sum" => {
            let total: f64 = args
                .get("values")
                .and_then(|v| v.as_array())
                .map(|a| a.iter().filter_map(|x| x.as_f64()).sum())
                .unwrap_or(0.0);
            Ok(json!({ "sum": total }))
        }
        other => Err(format!("unknown tool '{other}'").into()),
    }
}

#[derive(Debug)]
pub struct AgentHandler;
#[async_trait]
impl NodeHandler for AgentHandler {
    /// A bounded tool-use loop. On each step the agent gets a *directive* — JSON of the form
    /// `{ "tool": <name>, "args": {…} }` (run a built-in tool and continue) or `{ "final": <x> }`
    /// (stop with that answer). Directives come from the LLM provider, or, for deterministic
    /// offline runs/tests, from a `script` param (an array of directives applied in order).
    ///
    /// Params: `task`, `max_steps` (default 5), optional `model`, optional `script`.
    /// Built-in tools: `echo`, `upper`, `sum`.
    async fn execute(
        &self,
        node: &NodeSpec,
        _input: NodeInput,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let task = node
            .params
            .get("task")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let max_steps = node
            .params
            .get("max_steps")
            .and_then(|v| v.as_u64())
            .unwrap_or(5) as usize;
        let model = node
            .params
            .get("model")
            .and_then(|v| v.as_str())
            .unwrap_or("gpt-3.5-turbo")
            .to_string();
        let script = node
            .params
            .get("script")
            .and_then(|v| v.as_array())
            .cloned();

        let provider = provider_for(&model);
        let mut scratchpad: Vec<Value> = Vec::new();
        let mut answer = Value::Null;
        let mut steps = 0usize;
        let mut budget_exhausted = true;

        for step in 0..max_steps {
            steps = step + 1;
            // Obtain the next directive: from the script if provided, else from the LLM.
            let directive: Value = if let Some(s) = &script {
                s.get(step)
                    .cloned()
                    .unwrap_or(json!({ "final": Value::Null }))
            } else {
                let prompt = format!(
                    "Task: {task}\nScratchpad: {}",
                    serde_json::to_string(&scratchpad).unwrap_or_default()
                );
                let req = CompletionRequest {
                    model: model.clone(),
                    prompt,
                    system: Some(
                        "You are an agent. Reply ONLY with JSON: {\"tool\":..,\"args\":..} or \
                         {\"final\":..}."
                            .to_string(),
                    ),
                    temperature: None,
                };
                let text = provider.complete(&req).await?;
                serde_json::from_str(&text).unwrap_or(json!({ "final": text }))
            };

            if let Some(tool) = directive.get("tool").and_then(|t| t.as_str()) {
                let args = directive.get("args").cloned().unwrap_or_else(|| json!({}));
                let result = run_tool(tool, &args)?;
                scratchpad.push(json!({ "tool": tool, "args": args, "result": result }));
                continue;
            }
            // No tool requested → treat as the final answer.
            answer = directive.get("final").cloned().unwrap_or(directive);
            budget_exhausted = false;
            break;
        }

        Ok(json!({
            "agent": node.id,
            "steps": steps,
            "answer": answer,
            "scratchpad": scratchpad,
            "budget_exhausted": budget_exhausted,
        }))
    }
}

#[derive(Debug)]
pub struct EnsembleHandler;
#[async_trait]
impl NodeHandler for EnsembleHandler {
    /// Aggregates the outputs of its predecessors ("members") with a configurable `strategy`:
    /// - `collect` (default): emit all member outputs as an array plus a `count`.
    /// - `first`: emit the first member's output.
    /// - `vote`: majority vote over each member's `field` value (set via the `field` param);
    ///   emits the `winner` (the most common value) and the full `tally`.
    async fn execute(
        &self,
        node: &NodeSpec,
        input: NodeInput,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let strategy = node
            .params
            .get("strategy")
            .and_then(|v| v.as_str())
            .unwrap_or("collect");

        let members: Vec<Value> = input.sources.iter().map(|(_, out)| out.clone()).collect();

        match strategy {
            "first" => Ok(json!({
                "ensemble": node.id,
                "strategy": "first",
                "result": members.first().cloned().unwrap_or(Value::Null),
            })),
            "vote" => {
                let field = node
                    .params
                    .get("field")
                    .and_then(|v| v.as_str())
                    .ok_or("ensemble 'vote' requires a string 'field' param")?;
                let mut tally: BTreeMap<String, usize> = BTreeMap::new();
                for out in &members {
                    if let Some(v) = out.get(field) {
                        let key = match v {
                            Value::String(s) => s.clone(),
                            other => other.to_string(),
                        };
                        *tally.entry(key).or_insert(0) += 1;
                    }
                }
                // Highest count wins; ties broken by the (sorted) key for determinism.
                let winner = tally.iter().max_by_key(|(_, &c)| c).map(|(k, _)| k.clone());
                Ok(json!({
                    "ensemble": node.id,
                    "strategy": "vote",
                    "winner": winner,
                    "tally": tally,
                }))
            }
            _ => Ok(json!({
                "ensemble": node.id,
                "strategy": "collect",
                "count": members.len(),
                "members": members,
            })),
        }
    }
}

#[derive(Debug)]
pub struct RouterHandler;
#[async_trait]
impl NodeHandler for RouterHandler {
    /// Chooses a downstream route by switching on an input field.
    ///
    /// Params: `field` (input field to switch on), `routes` (object mapping the field's string
    /// value to a route label), and optional `default`. Emits `{ route }`; the engine then takes
    /// only the outgoing edge(s) whose `on` equals that route label.
    async fn execute(
        &self,
        node: &NodeSpec,
        input: NodeInput,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let field = node
            .params
            .get("field")
            .and_then(|v| v.as_str())
            .ok_or("router requires a string 'field' param")?;

        // Normalize the looked-up value to a string key (strings stay as-is; others via Display).
        let key = match input.merged.get(field) {
            Some(Value::String(s)) => s.clone(),
            Some(other) => other.to_string(),
            None => String::new(),
        };

        let default_route = node
            .params
            .get("default")
            .and_then(|v| v.as_str())
            .unwrap_or("default");
        let route = node
            .params
            .get("routes")
            .and_then(|r| r.get(&key))
            .and_then(|v| v.as_str())
            .unwrap_or(default_route)
            .to_string();

        Ok(json!({
            "router": node.id,
            "route": route,
        }))
    }
}

/// Handler for Subgraph nodes: runs a nested workflow and returns its result.
#[derive(Debug)]
pub struct SubgraphHandler;

#[async_trait]
impl NodeHandler for SubgraphHandler {
    /// Executes an inline nested workflow (the `workflow` param, a full workflow spec object) on a
    /// fresh in-memory engine, passing this node's merged input through as the nested run's input.
    async fn execute(
        &self,
        node: &NodeSpec,
        input: NodeInput,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let wf = node
            .params
            .get("workflow")
            .ok_or("subgraph requires a 'workflow' param (an inline workflow spec)")?;
        let wf_json = serde_json::to_string(wf)?;
        let dag = gaussflow_core::TypeSafeDag::from_json(&wf_json)
            .map_err(|e| format!("subgraph: invalid nested workflow: {e}"))?;

        let store = crate::InMemoryRunStore::new();
        let result = crate::execute_with_store(dag, input.merged, &store).await?;

        Ok(json!({
            "subgraph": node.id,
            "result": result.get("output").cloned().unwrap_or(Value::Null),
            "outputs": result.get("outputs").cloned().unwrap_or(Value::Null),
        }))
    }
}

// Handler for DataProcessor nodes
#[derive(Debug)]
pub struct DataProcessorHandler;

#[async_trait]
impl NodeHandler for DataProcessorHandler {
    /// Deterministic data transforms selected by the `op` param:
    /// - `passthrough` (default): emit the input unchanged under `result`.
    /// - `extract`: emit `input[field]` under `result` (`null` if absent).
    /// - `set`: shallow-merge the object in the `value` param over the input.
    async fn execute(
        &self,
        node: &NodeSpec,
        input: NodeInput,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let data = input.merged;
        let op = node
            .params
            .get("op")
            .and_then(|v| v.as_str())
            .unwrap_or("passthrough");

        let result = match op {
            "extract" => {
                let field = node
                    .params
                    .get("field")
                    .and_then(|v| v.as_str())
                    .ok_or("data_processor 'extract' requires a string 'field' param")?;
                data.get(field).cloned().unwrap_or(Value::Null)
            }
            "set" => {
                let mut merged = data.clone();
                if let (Some(obj), Some(patch)) = (
                    merged.as_object_mut(),
                    node.params.get("value").and_then(|v| v.as_object()),
                ) {
                    for (k, v) in patch {
                        obj.insert(k.clone(), v.clone());
                    }
                }
                merged
            }
            _ => data,
        };

        Ok(json!({
            "data_processor": node.id,
            "op": op,
            "result": result,
        }))
    }
}

// Handler for Conditional nodes
#[derive(Debug)]
pub struct ConditionalHandler;

#[async_trait]
impl NodeHandler for ConditionalHandler {
    /// Evaluates a single comparison over the input and reports the outcome.
    ///
    /// Params: `field` (path into the input object), `op` (`eq`|`ne`|`gt`|`lt`|`ge`|`le`), and
    /// `value` to compare against. Numbers compare numerically; `eq`/`ne` also work for any JSON
    /// value. Emits `{ matched, branch }` where `branch` is `"true"`/`"false"`; the engine then
    /// takes only the matching outgoing edge(s).
    async fn execute(
        &self,
        node: &NodeSpec,
        input: NodeInput,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let field = node
            .params
            .get("field")
            .and_then(|v| v.as_str())
            .ok_or("conditional requires a string 'field' param")?;
        let op = node
            .params
            .get("op")
            .and_then(|v| v.as_str())
            .unwrap_or("eq");
        let expected = node.params.get("value").cloned().unwrap_or(Value::Null);
        let actual = input.merged.get(field).cloned().unwrap_or(Value::Null);

        let matched = match op {
            "eq" => actual == expected,
            "ne" => actual != expected,
            "gt" | "lt" | "ge" | "le" => match (actual.as_f64(), expected.as_f64()) {
                (Some(a), Some(e)) => match op {
                    "gt" => a > e,
                    "lt" => a < e,
                    "ge" => a >= e,
                    _ => a <= e,
                },
                // Non-numeric operands can't be ordered.
                _ => false,
            },
            other => return Err(format!("conditional: unsupported op '{other}'").into()),
        };

        Ok(json!({
            "conditional": node.id,
            "matched": matched,
            "branch": if matched { "true" } else { "false" },
        }))
    }
}

// Handler for Parallel nodes
#[derive(Debug)]
pub struct ParallelHandler;

#[async_trait]
impl NodeHandler for ParallelHandler {
    /// Runs several inline sub-workflows concurrently, each fed this node's merged input, and
    /// collects their outputs in declaration order.
    ///
    /// Params: `branches` — an array of inline workflow specs.
    async fn execute(
        &self,
        node: &NodeSpec,
        input: NodeInput,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let branches = node
            .params
            .get("branches")
            .and_then(|v| v.as_array())
            .ok_or("parallel requires a 'branches' array of inline workflow specs")?;

        // Spawn each branch concurrently on its own in-memory engine.
        let mut handles = Vec::with_capacity(branches.len());
        for branch in branches {
            let wf_json = serde_json::to_string(branch)?;
            let branch_input = input.merged.clone();
            handles.push(tokio::spawn(async move {
                let dag = gaussflow_core::TypeSafeDag::from_json(&wf_json)
                    .map_err(|e| format!("parallel: invalid branch workflow: {e}"))?;
                let store = crate::InMemoryRunStore::new();
                crate::execute_with_store(dag, branch_input, &store)
                    .await
                    .map_err(|e| e.to_string())
            }));
        }

        let mut results = Vec::with_capacity(handles.len());
        for h in handles {
            let branch_result = h.await.map_err(|e| e.to_string())??;
            results.push(branch_result.get("output").cloned().unwrap_or(Value::Null));
        }

        Ok(json!({
            "parallel": node.id,
            "count": results.len(),
            "results": results,
        }))
    }
}

pub fn handler_for(kind: &NodeType) -> Box<dyn NodeHandler> {
    match kind {
        NodeType::LlmCall => Box::new(LlmCallHandler),
        NodeType::Agent => Box::new(AgentHandler),
        NodeType::Ensemble => Box::new(EnsembleHandler),
        NodeType::Router => Box::new(RouterHandler),
        NodeType::Subgraph => Box::new(SubgraphHandler),
        NodeType::DataProcessor => Box::new(DataProcessorHandler),
        NodeType::Conditional => Box::new(ConditionalHandler),
        NodeType::Parallel => Box::new(ParallelHandler),
    }
}
