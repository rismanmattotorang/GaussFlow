use crate::provider::{provider_for, CompletionRequest};
use async_trait::async_trait;
use gaussflow_core::model::{NodeConfig, NodeSpec, NodeType};
use serde_json::{json, Value};

#[async_trait]
pub trait NodeHandler: Send + Sync {
    async fn execute(
        &self,
        node: &NodeSpec,
        _input: Value,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>>;
}

#[derive(Debug)]
pub struct LlmCallHandler;
#[async_trait]
impl NodeHandler for LlmCallHandler {
    async fn execute(
        &self,
        node: &NodeSpec,
        _input: Value,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        // Get model from config or use default
        let model = match &node.config {
            NodeConfig::LlmCall { model, .. } => model.as_str(),
            NodeConfig::Generic(map) => map
                .get("model")
                .and_then(|m| m.as_str())
                .unwrap_or("gpt-3.5-turbo"),
            _ => "gpt-3.5-turbo",
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

#[derive(Debug)]
pub struct AgentHandler;
#[async_trait]
impl NodeHandler for AgentHandler {
    async fn execute(
        &self,
        node: &NodeSpec,
        _input: Value,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        Ok(json!({
            "agent": node.id,
            "state": _input
        }))
    }
}

#[derive(Debug)]
pub struct EnsembleHandler;
#[async_trait]
impl NodeHandler for EnsembleHandler {
    async fn execute(
        &self,
        node: &NodeSpec,
        _input: Value,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let out = json!({
            "ensemble": node.id,
            "children": _input
        });
        Ok(out)
    }
}

#[derive(Debug)]
pub struct RouterHandler;
#[async_trait]
impl NodeHandler for RouterHandler {
    async fn execute(
        &self,
        node: &NodeSpec,
        _input: Value,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        Ok(json!({
            "router": node.id,
            "routed": _input
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
        input: Value,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
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
                input.get(field).cloned().unwrap_or(Value::Null)
            }
            "set" => {
                let mut merged = input.clone();
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
            _ => input,
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
    /// value. Emits `{ matched, branch }` where `branch` is `"true"`/`"false"`.
    ///
    /// NOTE: this computes the decision; engine-level *edge skipping* based on the branch is a
    /// follow-up (conditional edge traversal) tracked in the roadmap.
    async fn execute(
        &self,
        node: &NodeSpec,
        input: Value,
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
        let actual = input.get(field).cloned().unwrap_or(Value::Null);

        let matched = match op {
            "eq" => actual == expected,
            "ne" => actual != expected,
            "gt" | "lt" | "ge" | "le" => {
                match (actual.as_f64(), expected.as_f64()) {
                    (Some(a), Some(e)) => match op {
                        "gt" => a > e,
                        "lt" => a < e,
                        "ge" => a >= e,
                        _ => a <= e,
                    },
                    // Non-numeric operands can't be ordered.
                    _ => false,
                }
            }
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
    async fn execute(
        &self,
        node: &NodeSpec,
        _input: Value,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        Ok(json!({
            "parallel": node.id,
            "input": _input
        }))
    }
}

pub fn handler_for(kind: &NodeType) -> Box<dyn NodeHandler> {
    match kind {
        NodeType::LlmCall => Box::new(LlmCallHandler),
        NodeType::Agent => Box::new(AgentHandler),
        NodeType::Ensemble => Box::new(EnsembleHandler),
        NodeType::Router => Box::new(RouterHandler),
        NodeType::Subgraph => Box::new(AgentHandler),
        NodeType::DataProcessor => Box::new(DataProcessorHandler),
        NodeType::Conditional => Box::new(ConditionalHandler),
        NodeType::Parallel => Box::new(ParallelHandler),
    }
}
