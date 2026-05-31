//! Offline test of provider selection by model name (no network calls are made).

use gaussflow_runtime::provider::provider_for;

#[test]
fn selects_provider_by_model_prefix() {
    assert_eq!(provider_for("mock-anything").name(), "mock");
    assert_eq!(provider_for("claude-3-5-sonnet").name(), "anthropic");
    assert_eq!(provider_for("anthropic.claude").name(), "anthropic");
    assert_eq!(provider_for("gpt-4o-mini").name(), "openai");
    assert_eq!(provider_for("some-other-model").name(), "openai");
}
