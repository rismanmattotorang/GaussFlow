use async_trait::async_trait;
use gaussflow_core::model::{NodeConfig, NodeSpec, NodeType};
use reqwest::Client;
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

        // Build request body
        let mut body = serde_json::json!({
            "model": model,
            "messages": [
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": node.params.get("prompt").cloned().unwrap_or(Value::String("Hello".into()))}
            ]
        });

        // Add temperature if specified
        if let Some(temp) = temperature {
            body["temperature"] = json!(temp);
        }

        let api_key = std::env::var("OPENAI_API_KEY").map_err(|_| "OPENAI_API_KEY not set")?;
        let client = Client::new();
        let resp = client
            .post("https://api.openai.com/v1/chat/completions")
            .bearer_auth(api_key)
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("Failed to send request to OpenAI API: {}", e))?
            .error_for_status()
            .map_err(|e| format!("OpenAI API returned an error: {}", e))?
            .json::<serde_json::Value>()
            .await
            .map_err(|e| format!("Failed to parse OpenAI API response: {}", e))?;

        let answer = resp["choices"][0]["message"]["content"]
            .as_str()
            .ok_or("Invalid response format from OpenAI API")?;

        Ok(json!({
            "llm_call": node.id,
            "model": model,
            "answer": answer
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
    async fn execute(
        &self,
        node: &NodeSpec,
        _input: Value,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        Ok(json!({
            "data_processor": node.id,
            "input": _input
        }))
    }
}

// Handler for Conditional nodes
#[derive(Debug)]
pub struct ConditionalHandler;

#[async_trait]
impl NodeHandler for ConditionalHandler {
    async fn execute(
        &self,
        node: &NodeSpec,
        _input: Value,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        Ok(json!({
            "conditional": node.id,
            "input": _input
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
