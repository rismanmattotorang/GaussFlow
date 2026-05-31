//! Pluggable run persistence.
//!
//! The execution engine depends only on the [`RunStore`] trait, not on any specific database.
//! This decouples execution from infrastructure: the default [`InMemoryRunStore`] needs **no
//! external services**, so a workflow can run with zero dependencies, while [`SurrealRunStore`]
//! provides durable persistence when configured. This is the Phase 1 "decouple execution from
//! SurrealDB" work (see `docs/PRODUCTION_ROADMAP.md`).

use async_trait::async_trait;
use serde_json::Value;
use std::error::Error;

/// Error type returned by run-store backends.
pub type StoreError = Box<dyn Error + Send + Sync>;

/// A persisted record of a single workflow run.
#[derive(Debug, Clone)]
pub struct RunRecord {
    /// Unique run identifier.
    pub run_id: String,
    /// Current status: `running`, `finished`, or `failed: <reason>`.
    pub status: String,
    /// The input the run started with.
    pub input: Value,
    /// The final output, once the run has finished.
    pub output: Option<Value>,
}

/// Persistence backend for workflow runs. The engine writes run lifecycle events here.
#[async_trait]
pub trait RunStore: Send + Sync {
    /// Record that a run has started.
    async fn start_run(&self, run_id: &str, input: &Value) -> Result<(), StoreError>;
    /// Record that a run finished successfully with the given output.
    async fn finish_run(&self, run_id: &str, output: &Value) -> Result<(), StoreError>;
    /// Record that a run failed.
    async fn fail_run(&self, run_id: &str, error: &str) -> Result<(), StoreError>;
}

/// Default, dependency-free run store. Keeps run records in memory.
#[derive(Debug, Default)]
pub struct InMemoryRunStore {
    runs: dashmap::DashMap<String, RunRecord>,
}

impl InMemoryRunStore {
    /// Create an empty in-memory store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Fetch a run record by id (useful for tests and inspection).
    pub fn get(&self, run_id: &str) -> Option<RunRecord> {
        self.runs.get(run_id).map(|r| r.clone())
    }

    /// Number of runs recorded.
    pub fn len(&self) -> usize {
        self.runs.len()
    }

    /// Whether no runs have been recorded.
    pub fn is_empty(&self) -> bool {
        self.runs.is_empty()
    }
}

#[async_trait]
impl RunStore for InMemoryRunStore {
    async fn start_run(&self, run_id: &str, input: &Value) -> Result<(), StoreError> {
        self.runs.insert(
            run_id.to_string(),
            RunRecord {
                run_id: run_id.to_string(),
                status: "running".to_string(),
                input: input.clone(),
                output: None,
            },
        );
        Ok(())
    }

    async fn finish_run(&self, run_id: &str, output: &Value) -> Result<(), StoreError> {
        if let Some(mut r) = self.runs.get_mut(run_id) {
            r.status = "finished".to_string();
            r.output = Some(output.clone());
        }
        Ok(())
    }

    async fn fail_run(&self, run_id: &str, error: &str) -> Result<(), StoreError> {
        if let Some(mut r) = self.runs.get_mut(run_id) {
            r.status = format!("failed: {error}");
        }
        Ok(())
    }
}

/// SurrealDB-backed run store. Connects once using environment-sourced settings
/// (see [`crate::surreal_settings`]) and writes run lifecycle events as `run` records.
pub struct SurrealRunStore {
    db: surrealdb::Surreal<surrealdb::engine::remote::ws::Client>,
}

impl SurrealRunStore {
    /// Connect to SurrealDB using environment-sourced settings.
    pub async fn connect() -> Result<Self, StoreError> {
        use surrealdb::engine::remote::ws::Ws;
        use surrealdb::opt::auth::Root;

        let (url, user, pass, ns, db_name) = crate::surreal_settings();
        let db = surrealdb::Surreal::new::<Ws>(url.as_str()).await?;
        db.signin(Root {
            username: user.as_str(),
            password: pass.as_str(),
        })
        .await?;
        db.use_ns(ns.as_str()).use_db(db_name.as_str()).await?;
        Ok(Self { db })
    }
}

#[async_trait]
impl RunStore for SurrealRunStore {
    async fn start_run(&self, run_id: &str, input: &Value) -> Result<(), StoreError> {
        self.db
            .query(
                "CREATE run SET id = $id, status = 'running', started = time::now(), input = $input",
            )
            .bind(("id", run_id.to_string()))
            .bind(("input", input.clone()))
            .await?;
        Ok(())
    }

    async fn finish_run(&self, run_id: &str, output: &Value) -> Result<(), StoreError> {
        self.db
            .query(
                "UPDATE type::thing('run', $id) SET status='finished', finished=time::now(), output=$out",
            )
            .bind(("id", run_id.to_string()))
            .bind(("out", output.clone()))
            .await?;
        Ok(())
    }

    async fn fail_run(&self, run_id: &str, error: &str) -> Result<(), StoreError> {
        self.db
            .query(
                "UPDATE type::thing('run', $id) SET status='failed', finished=time::now(), error=$err",
            )
            .bind(("id", run_id.to_string()))
            .bind(("err", error.to_string()))
            .await?;
        Ok(())
    }
}
