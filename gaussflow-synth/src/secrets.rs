//! Secret resolution for deployments.
//!
//! A deployment records the *names* of the secrets its workflow needs (e.g. `OPENAI_API_KEY`).
//! At run time those names are resolved through a [`SecretProvider`] — the environment by default,
//! a vault/cloud secret manager in production, or a fixed map in tests — so secrets are never
//! baked into the spec.

use std::collections::HashMap;

/// Resolves secret values by name.
pub trait SecretProvider: Send + Sync {
    /// Return the secret for `key`, if available.
    fn get(&self, key: &str) -> Option<String>;
}

/// Resolves secrets from process environment variables (the default).
#[derive(Debug, Default)]
pub struct EnvSecretProvider;

impl SecretProvider for EnvSecretProvider {
    fn get(&self, key: &str) -> Option<String> {
        std::env::var(key).ok()
    }
}

/// Resolves secrets from an in-memory map (useful for tests and injection).
#[derive(Debug, Default, Clone)]
pub struct MapSecretProvider(pub HashMap<String, String>);

impl MapSecretProvider {
    /// Build from key/value pairs.
    pub fn from_pairs<I, K, V>(pairs: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        Self(
            pairs
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        )
    }
}

impl SecretProvider for MapSecretProvider {
    fn get(&self, key: &str) -> Option<String> {
        self.0.get(key).cloned()
    }
}

/// The environment-variable name of the secret a given model needs, if any.
///
/// `gpt*` → `OPENAI_API_KEY`, `claude*`/`anthropic*` → `ANTHROPIC_API_KEY`; `mock*` and `ollama*`
/// (local) need no secret.
pub fn secret_for_model(model: &str) -> Option<&'static str> {
    let m = model.to_ascii_lowercase();
    if m.starts_with("mock") || m.starts_with("ollama") {
        None
    } else if m.starts_with("claude") || m.starts_with("anthropic") {
        Some("ANTHROPIC_API_KEY")
    } else {
        // Default family is OpenAI (gpt-*, o*, etc.).
        Some("OPENAI_API_KEY")
    }
}
