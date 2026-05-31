//! LLM provider abstraction (Phase 2).
//!
//! Node handlers depend on the [`LlmProvider`] trait, not on any specific vendor SDK. This lets
//! GaussFlow target multiple providers and — importantly — run **offline, deterministic tests**
//! via [`MockProvider`]. [`provider_for`] selects a backend from the model name / environment.

use async_trait::async_trait;
use serde_json::json;

/// Error returned by an LLM provider.
pub type ProviderError = Box<dyn std::error::Error + Send + Sync>;

/// A single completion request, provider-agnostic.
#[derive(Debug, Clone)]
pub struct CompletionRequest {
    /// Model identifier (e.g. `gpt-4o-mini`, `mock`).
    pub model: String,
    /// The user prompt.
    pub prompt: String,
    /// Optional system prompt.
    pub system: Option<String>,
    /// Optional sampling temperature.
    pub temperature: Option<f32>,
}

/// A pluggable large-language-model backend.
#[async_trait]
pub trait LlmProvider: Send + Sync {
    /// Produce a completion for `req`.
    async fn complete(&self, req: &CompletionRequest) -> Result<String, ProviderError>;
    /// A short, stable name for this provider (surfaced in node output).
    fn name(&self) -> &'static str;
}

/// Select a provider for a given model.
///
/// `GAUSSFLOW_LLM_PROVIDER` (`mock` | `openai`) takes precedence; otherwise a model whose name
/// starts with `mock` uses the deterministic [`MockProvider`], and everything else uses OpenAI.
pub fn provider_for(model: &str) -> Box<dyn LlmProvider> {
    if let Ok(p) = std::env::var("GAUSSFLOW_LLM_PROVIDER") {
        match p.to_ascii_lowercase().as_str() {
            "mock" => return Box::new(MockProvider),
            "openai" => return Box::new(OpenAiProvider),
            _ => {}
        }
    }
    if model.starts_with("mock") {
        Box::new(MockProvider)
    } else {
        Box::new(OpenAiProvider)
    }
}

/// Deterministic, offline provider. Never makes a network call; echoes a stable transformation of
/// the request. Used for tests and for running GaussFlow without any API key.
#[derive(Debug, Default)]
pub struct MockProvider;

#[async_trait]
impl LlmProvider for MockProvider {
    async fn complete(&self, req: &CompletionRequest) -> Result<String, ProviderError> {
        Ok(format!("[mock:{}] {}", req.model, req.prompt))
    }

    fn name(&self) -> &'static str {
        "mock"
    }
}

/// OpenAI Chat Completions provider. Reads `OPENAI_API_KEY` from the environment.
#[derive(Debug, Default)]
pub struct OpenAiProvider;

#[async_trait]
impl LlmProvider for OpenAiProvider {
    async fn complete(&self, req: &CompletionRequest) -> Result<String, ProviderError> {
        let mut body = json!({
            "model": req.model,
            "messages": [
                {"role": "system", "content": req.system.clone().unwrap_or_else(|| "You are a helpful assistant.".to_string())},
                {"role": "user", "content": req.prompt},
            ]
        });
        if let Some(temp) = req.temperature {
            body["temperature"] = json!(temp);
        }

        let api_key = std::env::var("OPENAI_API_KEY").map_err(|_| "OPENAI_API_KEY not set")?;
        let client = reqwest::Client::new();
        let resp = client
            .post("https://api.openai.com/v1/chat/completions")
            .bearer_auth(api_key)
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("Failed to send request to OpenAI API: {e}"))?
            .error_for_status()
            .map_err(|e| format!("OpenAI API returned an error: {e}"))?
            .json::<serde_json::Value>()
            .await
            .map_err(|e| format!("Failed to parse OpenAI API response: {e}"))?;

        resp["choices"][0]["message"]["content"]
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| "Invalid response format from OpenAI API".into())
    }

    fn name(&self) -> &'static str {
        "openai"
    }
}
