//! Phase S ops: required-secrets analysis, secret resolution, quotas, and trigger registration.

use gaussflow_synth::deploy::{
    check_quota, deploy_with, required_secrets, DeployOptions, Quota, Trigger,
};
use gaussflow_synth::{
    validate_plan, Constraints, InMemoryDeploymentStore, MapSecretProvider, PlanIR,
};
use serde_json::json;

fn result_with(steps: serde_json::Value) -> gaussflow_synth::SynthesisResult {
    let plan: PlanIR = serde_json::from_value(json!({ "name": "ops", "steps": steps })).unwrap();
    validate_plan(&plan, &Constraints::default(), "ops goal").unwrap()
}

#[test]
fn required_secrets_are_inferred_from_models() {
    let r = result_with(json!([
        { "id": "a", "capability": "llm_call", "depends_on": [], "params": { "model": "gpt-4o-mini", "prompt": "x" } },
        { "id": "b", "capability": "llm_call", "depends_on": ["a"], "params": { "model": "claude-3-5-sonnet", "prompt": "y" } },
        { "id": "c", "capability": "llm_call", "depends_on": ["b"], "params": { "model": "mock-z", "prompt": "z" } }
    ]));
    let secrets = required_secrets(&r.spec);
    assert!(secrets.contains(&"OPENAI_API_KEY".to_string()));
    assert!(secrets.contains(&"ANTHROPIC_API_KEY".to_string()));
    // mock needs none; set has exactly the two real ones (deduped/sorted).
    assert_eq!(secrets.len(), 2);
}

#[tokio::test]
async fn deploy_records_required_secrets_and_check_secrets_works() {
    let store = InMemoryDeploymentStore::new();
    let r = result_with(json!([
        { "id": "a", "capability": "llm_call", "depends_on": [], "params": { "model": "gpt-4o-mini", "prompt": "x" } }
    ]));
    let dep = deploy_with(&r, "p", DeployOptions::default(), &store)
        .await
        .unwrap();
    assert_eq!(dep.required_secrets, vec!["OPENAI_API_KEY".to_string()]);

    // Missing secret → reported; present secret → ok.
    let empty = MapSecretProvider::default();
    assert_eq!(
        dep.check_secrets(&empty).unwrap_err(),
        vec!["OPENAI_API_KEY".to_string()]
    );
    let provided = MapSecretProvider::from_pairs([("OPENAI_API_KEY", "sk-test")]);
    assert!(dep.check_secrets(&provided).is_ok());
}

#[test]
fn quota_rejects_oversized_or_external_plans() {
    let r = result_with(json!([
        { "id": "a", "capability": "llm_call", "depends_on": [], "params": { "model": "gpt-4o-mini", "prompt": "x" } }
    ]));
    // Node-count quota.
    assert!(check_quota(
        &r.estimate,
        &Quota {
            max_nodes: Some(0),
            ..Default::default()
        }
    )
    .is_err());
    // Model-invocation quota.
    assert!(check_quota(
        &r.estimate,
        &Quota {
            max_model_invocations: Some(0),
            ..Default::default()
        }
    )
    .is_err());
    // External-calls ban (llm_call makes external calls).
    assert!(check_quota(
        &r.estimate,
        &Quota {
            allow_external_calls: false,
            ..Default::default()
        }
    )
    .is_err());
    // Generous quota passes.
    assert!(check_quota(
        &r.estimate,
        &Quota {
            max_nodes: Some(10),
            max_model_invocations: Some(10),
            allow_external_calls: true
        }
    )
    .is_ok());
}

#[tokio::test]
async fn deploy_with_quota_and_triggers() {
    let store = InMemoryDeploymentStore::new();
    let r = result_with(json!([
        { "id": "a", "capability": "data_processor", "depends_on": [], "params": {} }
    ]));

    // A quota that's exceeded blocks deployment.
    let blocked = deploy_with(
        &r,
        "p",
        DeployOptions {
            quota: Some(Quota {
                max_nodes: Some(0),
                ..Default::default()
            }),
            triggers: vec![],
        },
        &store,
    )
    .await;
    assert!(blocked.is_err());

    // Within quota, with a schedule trigger registered.
    let dep = deploy_with(
        &r,
        "p",
        DeployOptions {
            quota: Some(Quota {
                max_nodes: Some(5),
                ..Default::default()
            }),
            triggers: vec![Trigger::Schedule("0 9 * * *".into())],
        },
        &store,
    )
    .await
    .unwrap();
    assert_eq!(dep.triggers, vec![Trigger::Schedule("0 9 * * *".into())]);
}
