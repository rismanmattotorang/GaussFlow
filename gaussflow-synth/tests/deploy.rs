//! Phase S deploy hardening: versioned, immutable deployments with provenance and run trace-back.

use gaussflow_synth::deploy::{
    deploy, run_deployment, Deployment, DeploymentStore, FileDeploymentStore,
};
use gaussflow_synth::{validate_plan, Constraints, InMemoryDeploymentStore, PlanIR};
use serde_json::json;

fn sample_result(name: &str) -> gaussflow_synth::SynthesisResult {
    let plan: PlanIR = serde_json::from_value(json!({
        "name": name,
        "steps": [
            { "id": "ingest", "capability": "data_processor", "depends_on": [],
              "params": { "op": "set", "value": { "ok": true } } }
        ]
    }))
    .unwrap();
    validate_plan(&plan, &Constraints::default(), "do the thing").unwrap()
}

#[tokio::test]
async fn deploy_assigns_versions_and_records_provenance() {
    let store = InMemoryDeploymentStore::new();
    let result = sample_result("pipeline");

    let d1 = deploy(&result, "first prompt", &store).await.unwrap();
    let d2 = deploy(&result, "second prompt", &store).await.unwrap();

    assert_eq!(d1.id, "pipeline-v1");
    assert_eq!(d2.id, "pipeline-v2");
    assert_eq!(d1.version, 1);
    assert_eq!(d2.version, 2);
    // Provenance: the originating prompt is stored on the deployment.
    assert_eq!(d1.prompt, "first prompt");
    // Content hash is stable for identical specs.
    assert_eq!(d1.spec_hash, d2.spec_hash);
    assert_eq!(store.list().await.unwrap().len(), 2);
}

#[tokio::test]
async fn deployments_are_immutable() {
    let store = InMemoryDeploymentStore::new();
    // Hand-craft two deployments with the same id but different content.
    let a = Deployment {
        id: "x-v1".into(),
        name: "x".into(),
        version: 1,
        prompt: "p".into(),
        spec_json: r#"{"a":1}"#.into(),
        spec_hash: "hash-a".into(),
        created_at_unix: 0,
    };
    let b = Deployment {
        spec_json: r#"{"a":2}"#.into(),
        spec_hash: "hash-b".into(),
        ..a.clone()
    };
    store.put(&a).await.unwrap();
    // Re-putting identical content is a no-op; different content under the same id is rejected.
    store.put(&a).await.unwrap();
    assert!(store.put(&b).await.is_err());
}

#[tokio::test]
async fn run_deployment_links_runs_back_to_the_deployment() {
    let store = InMemoryDeploymentStore::new();
    let deployment = deploy(&sample_result("traced"), "trace me", &store)
        .await
        .unwrap();

    let out = run_deployment(&deployment, json!({}), &store)
        .await
        .unwrap();
    let run_id = out["run_id"].as_str().unwrap();

    // The run is linked to the deployment (and thus its prompt) for audit.
    let runs = store.runs_of(&deployment.id).await.unwrap();
    assert_eq!(runs, vec![run_id.to_string()]);
}

#[tokio::test]
async fn file_store_persists_deployments_and_runs() {
    let dir = std::env::temp_dir().join(format!("gf-deploy-{}", std::process::id()));
    let store = FileDeploymentStore::new(&dir).unwrap();

    let deployment = deploy(&sample_result("filed"), "persist me", &store)
        .await
        .unwrap();

    // A fresh store over the same dir sees the persisted deployment.
    let reopened = FileDeploymentStore::new(&dir).unwrap();
    let got = reopened.get(&deployment.id).await.unwrap().unwrap();
    assert_eq!(got, deployment);

    run_deployment(&deployment, json!({}), &reopened)
        .await
        .unwrap();
    assert_eq!(reopened.runs_of(&deployment.id).await.unwrap().len(), 1);

    let _ = std::fs::remove_dir_all(&dir);
}
