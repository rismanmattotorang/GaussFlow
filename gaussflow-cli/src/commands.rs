use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use console::style;
use indicatif::ProgressBar;
use std::path::PathBuf;
use std::time::Instant;
use tracing::warn;

use crate::database::WorkflowRun;

use super::AppState;

// Command structures

#[derive(Args, Debug)]
pub struct SynthCommand {
    /// Natural-language description of the workflow to build
    #[arg(value_name = "PROMPT")]
    pub prompt: String,

    /// Planning model (use a `mock*` model name for offline experiments)
    #[arg(long, default_value = "gpt-4o-mini")]
    pub model: String,

    /// Deploy and run the synthesized workflow after showing the plan
    #[arg(long)]
    pub run: bool,

    /// JSON input to pass to the run (used with --run)
    #[arg(long)]
    pub input: Option<String>,

    /// Write the synthesized workflow spec to a file (for editing, then `gaussflow run <file>`)
    #[arg(long, value_name = "PATH")]
    pub save: Option<PathBuf>,

    /// Persist the confirmed workflow as a versioned, immutable deployment (with prompt provenance)
    #[arg(long)]
    pub deploy: bool,

    /// Directory for persisted deployments
    #[arg(long, value_name = "DIR", default_value = ".gaussflow/deployments")]
    pub deploy_dir: PathBuf,

    /// Register a cron schedule trigger on the deployment (with --deploy)
    #[arg(long, value_name = "CRON")]
    pub schedule: Option<String>,

    /// Reject the deployment if its estimated model-invocation upper bound exceeds this (quota)
    #[arg(long, value_name = "N")]
    pub max_model_calls: Option<usize>,
}

#[derive(Args, Debug)]
pub struct TokenCommand {
    /// Subject (principal id) the token is issued to
    #[arg(long, default_value = "admin")]
    pub sub: String,

    /// Role to grant (repeatable): viewer | operator | admin
    #[arg(long = "role")]
    pub roles: Vec<String>,

    /// Token lifetime in seconds
    #[arg(long, default_value = "3600")]
    pub ttl: u64,
}

#[derive(Args, Debug)]
pub struct ValidateCommand {
    /// Path to the workflow JSON file
    #[arg(value_name = "WORKFLOW")]
    pub workflow_file: PathBuf,

    /// Enable strict validation
    #[arg(short, long)]
    pub strict: bool,

    /// Output format (json, yaml, text)
    #[arg(short, long, default_value = "text")]
    pub format: String,
}

#[derive(Args, Debug)]
pub struct RunCommand {
    /// Path to the workflow JSON file
    #[arg(value_name = "WORKFLOW")]
    pub workflow_file: PathBuf,

    /// Optional JSON string to pass as input
    #[arg(short, long)]
    pub input: Option<String>,

    /// Enable parallel execution
    #[arg(short, long)]
    pub parallel: bool,

    /// Maximum concurrent workflows
    #[arg(long, default_value = "10")]
    pub max_concurrent: usize,

    /// Enable caching
    // `long`-only: the global `--config` already owns the short `-c`.
    #[arg(long)]
    pub cache: bool,

    /// Output format (json, yaml, text)
    #[arg(short, long, default_value = "text")]
    pub format: String,

    /// Save results to database
    #[arg(long)]
    pub save: bool,
}

#[derive(Args, Debug)]
pub struct TemplateCommand {
    #[command(subcommand)]
    pub subcommand: TemplateSubcommand,
}

#[derive(Subcommand, Debug)]
pub enum TemplateSubcommand {
    /// List available templates
    List {
        /// Filter by tags
        #[arg(short, long)]
        tags: Option<Vec<String>>,

        /// Output format
        #[arg(short, long, default_value = "table")]
        format: String,
    },

    /// Create a new template
    Create {
        /// Template name
        #[arg(short, long)]
        name: String,

        /// Template description
        #[arg(short, long)]
        description: Option<String>,

        /// Template file path
        #[arg(short, long)]
        file: PathBuf,

        /// Template tags
        #[arg(short, long)]
        tags: Option<Vec<String>>,

        /// Template version
        #[arg(short, long, default_value = "1.0.0")]
        version: String,
    },
}

#[derive(Args, Debug)]
pub struct MonitorCommand {
    #[command(subcommand)]
    pub subcommand: MonitorSubcommand,
}

#[derive(Subcommand, Debug)]
pub enum MonitorSubcommand {
    /// Show running workflows
    List {
        /// Show detailed information
        #[arg(short, long)]
        detailed: bool,

        /// Refresh interval in seconds
        #[arg(short, long, default_value = "5")]
        interval: u64,
    },

    /// Show workflow details
    Show {
        /// Workflow ID
        #[arg(value_name = "ID")]
        id: String,
    },
}

#[derive(Args, Debug)]
pub struct HistoryCommand {
    #[command(subcommand)]
    pub subcommand: HistorySubcommand,
}

#[derive(Subcommand, Debug)]
pub enum HistorySubcommand {
    /// List workflow history
    List {
        /// Filter by workflow ID
        #[arg(short, long)]
        workflow_id: Option<String>,

        /// Filter by status
        #[arg(short, long)]
        status: Option<String>,

        /// Limit number of results
        #[arg(short, long, default_value = "50")]
        limit: usize,

        /// Output format
        #[arg(short, long, default_value = "table")]
        format: String,
    },
}

#[derive(Args, Debug)]
pub struct ConfigCommand {
    #[command(subcommand)]
    pub subcommand: ConfigSubcommand,
}

#[derive(Subcommand, Debug)]
pub enum ConfigSubcommand {
    /// Show current configuration
    Show {
        /// Show sensitive values
        #[arg(short, long)]
        sensitive: bool,

        /// Output format
        #[arg(short, long, default_value = "toml")]
        format: String,
    },

    /// Set configuration value
    Set {
        /// Configuration key (e.g., database.url)
        #[arg(value_name = "KEY")]
        key: String,

        /// Configuration value
        #[arg(value_name = "VALUE")]
        value: String,
    },
}

#[derive(Args, Debug)]
pub struct ServeCommand {
    /// Port to bind
    #[arg(short, long, default_value = "3030")]
    pub port: u16,

    /// Host to bind
    #[arg(long, default_value = "0.0.0.0")]
    pub host: String,

    /// Enable CORS
    #[arg(long)]
    pub cors: bool,

    /// Enable authentication
    #[arg(long)]
    pub auth: bool,

    /// Enable metrics endpoint
    #[arg(long)]
    pub metrics: bool,

    /// Enable health check endpoint
    #[arg(long)]
    pub health: bool,
}

#[derive(Args, Debug)]
pub struct BenchCommand {
    /// Workflow file to benchmark
    #[arg(value_name = "WORKFLOW")]
    pub workflow_file: PathBuf,

    /// Number of iterations
    #[arg(short, long, default_value = "100")]
    pub iterations: usize,

    /// Number of concurrent workers
    #[arg(short, long, default_value = "4")]
    pub workers: usize,
}

#[derive(Args, Debug)]
pub struct DbCommand {
    #[command(subcommand)]
    pub subcommand: DbSubcommand,
}

#[derive(Subcommand, Debug)]
pub enum DbSubcommand {
    /// Initialize database
    Init {
        /// Force reinitialization
        #[arg(short, long)]
        force: bool,
    },

    /// Show database status
    Status {
        /// Show detailed information
        #[arg(short, long)]
        detailed: bool,
    },
}

#[derive(Args, Debug)]
pub struct CacheCommand {
    #[command(subcommand)]
    pub subcommand: CacheSubcommand,
}

#[derive(Subcommand, Debug)]
pub enum CacheSubcommand {
    /// Show cache statistics
    Stats {
        /// Show detailed information
        #[arg(short, long)]
        detailed: bool,
    },

    /// Clear cache
    Clear {
        /// Clear specific cache layer
        #[arg(short, long)]
        layer: Option<String>,
    },
}

// Command implementations

pub async fn validate_workflow(
    cmd: ValidateCommand,
    _state: AppState,
    pb: ProgressBar,
) -> Result<()> {
    pb.set_message("Reading workflow file...");

    let json_str = tokio::fs::read_to_string(&cmd.workflow_file)
        .await
        .with_context(|| {
            format!(
                "Failed to read workflow file: {}",
                cmd.workflow_file.display()
            )
        })
        .map_err(|e| {
            anyhow::anyhow!("File system error: {}", e).context("Workflow file could not be read")
        })?;

    pb.set_message("Parsing workflow...");

    let dag = gaussflow_core::TypeSafeDag::from_json(&json_str)
        .with_context(|| format!("Failed to parse workflow: {}", cmd.workflow_file.display()))
        .map_err(|e| {
            anyhow::anyhow!("Validation error: {}", e)
                .context("Workflow JSON is invalid or malformed")
        })?;

    pb.set_message("Validating workflow structure...");

    // Perform additional validation if strict mode is enabled
    if cmd.strict {
        validate_workflow_strict(&dag).await.map_err(|e| {
            anyhow::anyhow!("Strict validation error: {}", e)
                .context("Workflow failed strict validation checks")
        })?;
    }

    pb.set_message("Validation completed");

    // Output results based on format
    match cmd.format.as_str() {
        "json" => {
            let result = serde_json::json!({
                "valid": true,
                "name": dag.name,
                "nodes": dag.graph.node_count(),
                "edges": dag.graph.edge_count(),
                "strict_validation": cmd.strict
            });
            println!(
                "{}",
                serde_json::to_string_pretty(&result)
                    .map_err(|e| anyhow::anyhow!("Serialization error: {}", e))?
            );
        }
        "yaml" => {
            let result = serde_yaml::to_string(&serde_json::json!({
                "valid": true,
                "name": dag.name,
                "nodes": dag.graph.node_count(),
                "edges": dag.graph.edge_count(),
                "strict_validation": cmd.strict
            }))
            .map_err(|e| anyhow::anyhow!("YAML serialization error: {}", e))?;
            println!("{}", result);
        }
        _ => {
            println!(
                "{}",
                style("✓").green().bold().to_string()
                    + &format!(
                        " Successfully validated '{}' – nodes: {}, edges: {}",
                        dag.name,
                        dag.graph.node_count(),
                        dag.graph.edge_count()
                    )
            );
        }
    }

    Ok(())
}

pub async fn run_workflow(cmd: RunCommand, state: AppState, pb: ProgressBar) -> Result<()> {
    pb.set_message("Reading workflow file...");

    let json_str = tokio::fs::read_to_string(&cmd.workflow_file)
        .await
        .with_context(|| {
            format!(
                "Failed to read workflow file: {}",
                cmd.workflow_file.display()
            )
        })
        .map_err(|e| {
            anyhow::anyhow!("File system error: {}", e).context("Workflow file could not be read")
        })?;

    pb.set_message("Parsing workflow...");

    let dag = gaussflow_core::TypeSafeDag::from_json(&json_str)
        .with_context(|| format!("Failed to parse workflow: {}", cmd.workflow_file.display()))
        .map_err(|e| {
            anyhow::anyhow!("Validation error: {}", e)
                .context("Workflow JSON is invalid or malformed")
        })?;

    pb.set_message("Preparing input...");

    let input_val = cmd
        .input
        .map_or(Ok(serde_json::Value::Null), |i| serde_json::from_str(&i))
        .context("Failed to parse input JSON")
        .map_err(|e| {
            anyhow::anyhow!("Input parsing error: {}", e)
                .context("Input JSON is invalid or malformed")
        })?;

    // Record workflow start metrics
    state
        .metrics
        .record_workflow_start(&dag.name)
        .await
        .map_err(|e| {
            anyhow::anyhow!("Metrics error: {}", e)
                .context("Failed to record workflow start metrics")
        })?;

    pb.set_message("Executing workflow...");

    let start_time = Instant::now();
    let dag_name = dag.name.clone();
    let result = gaussflow_runtime::execute(dag, input_val.clone())
        .await
        .map_err(|e| {
            anyhow::anyhow!("Runtime execution error: {}", e).context("Workflow execution failed")
        })?;
    let execution_time = start_time.elapsed();

    // Record workflow completion metrics
    state
        .metrics
        .record_workflow_completion(&dag_name, execution_time, true)
        .await
        .map_err(|e| {
            anyhow::anyhow!("Metrics error: {}", e)
                .context("Failed to record workflow completion metrics")
        })?;

    // Save to database if requested
    if cmd.save {
        pb.set_message("Saving results to database...");

        let run = WorkflowRun {
            id: None,
            workflow_id: dag_name,
            status: "completed".to_string(),
            started_at: chrono::Utc::now(),
            finished_at: Some(chrono::Utc::now()),
            input: input_val,
            output: Some(result.clone()),
            error: None,
            execution_time: Some(execution_time),
            metadata: std::collections::HashMap::new(),
        };

        state.database.save_workflow_run(run).await.map_err(|e| {
            anyhow::anyhow!("Database error: {}", e)
                .context("Failed to save workflow run to database")
        })?;
    }

    pb.set_message("Workflow execution completed");

    // Output results based on format
    match cmd.format.as_str() {
        "json" => {
            println!(
                "{}",
                serde_json::to_string_pretty(&result)
                    .map_err(|e| anyhow::anyhow!("JSON serialization error: {}", e))?
            );
        }
        "yaml" => {
            let yaml = serde_yaml::to_string(&result)
                .map_err(|e| anyhow::anyhow!("YAML serialization error: {}", e))?;
            println!("{}", yaml);
        }
        _ => {
            println!(
                "\n{}",
                style("Workflow completed successfully!").green().bold()
            );
            println!("Execution time: {:?}", execution_time);
            println!(
                "Output:\n{}",
                serde_json::to_string_pretty(&result)
                    .map_err(|e| anyhow::anyhow!("Output serialization error: {}", e))?
            );
        }
    }

    Ok(())
}

/// Mint a JWT for the authenticated API. Requires `GAUSSFLOW_JWT_SECRET` (same secret the server
/// uses). Prints the token to stdout; pass it as `Authorization: Bearer <token>`.
pub async fn mint_token(cmd: TokenCommand) -> Result<()> {
    let secret = gaussflow_security::auth::secret_from_env()
        .map_err(|_| anyhow::anyhow!("set GAUSSFLOW_JWT_SECRET to mint a token"))?;
    let roles = if cmd.roles.is_empty() {
        vec!["admin".to_string()]
    } else {
        cmd.roles
    };
    let token = gaussflow_security::auth::mint(&secret, &cmd.sub, &roles, cmd.ttl)
        .map_err(|e| anyhow::anyhow!("failed to mint token: {e}"))?;
    println!("{token}");
    Ok(())
}

/// Synthesize a workflow from a natural-language prompt: prompt → DAG → (confirm) → deploy → run.
pub async fn synthesize_workflow(
    cmd: SynthCommand,
    _state: AppState,
    pb: ProgressBar,
) -> Result<()> {
    pb.set_message("Synthesizing workflow from prompt...");

    let provider = gaussflow_runtime::provider::provider_for(&cmd.model);
    let synth = gaussflow_synth::Synthesizer::new(provider.as_ref()).with_model(&cmd.model);
    let request = gaussflow_synth::SynthesisRequest::new(&cmd.prompt);

    let result = synth
        .synthesize(&request)
        .await
        .map_err(|e| anyhow::anyhow!("Synthesis failed: {e}"))?;

    // Confirmation view: the human-readable plan, the cost/latency estimate, and the spec.
    pb.set_message("Synthesis complete");
    println!("\n{}", style("Proposed plan").bold().underlined());
    println!("{}", result.explanation);
    println!("{} {}", style("Estimate:").bold(), result.estimate.summary);
    println!("\n{}", style("Workflow specification").bold().underlined());
    println!("{}", serde_json::to_string_pretty(&result.spec)?);

    if let Some(path) = &cmd.save {
        tokio::fs::write(path, &result.spec_json)
            .await
            .with_context(|| format!("failed to write spec to {}", path.display()))?;
        println!("\n{} {}", style("Saved spec to").green(), path.display());
    }

    // Deploy: persist the confirmed workflow as a versioned, immutable deployment with provenance.
    let deployment = if cmd.deploy {
        let store = gaussflow_synth::deploy::FileDeploymentStore::new(&cmd.deploy_dir)
            .map_err(|e| anyhow::anyhow!("could not open deployment store: {e}"))?;
        let options = gaussflow_synth::deploy::DeployOptions {
            quota: cmd.max_model_calls.map(|n| gaussflow_synth::deploy::Quota {
                max_model_invocations: Some(n),
                ..Default::default()
            }),
            triggers: cmd
                .schedule
                .clone()
                .map(gaussflow_synth::deploy::Trigger::Schedule)
                .into_iter()
                .collect(),
        };
        let dep = gaussflow_synth::deploy::deploy_with(&result, &cmd.prompt, options, &store)
            .await
            .map_err(|e| anyhow::anyhow!("deploy failed: {e}"))?;
        println!(
            "\n{} {} (version {}, hash {}…) in {}",
            style("Deployed").green().bold(),
            dep.id,
            dep.version,
            &dep.spec_hash[..12.min(dep.spec_hash.len())],
            cmd.deploy_dir.display()
        );
        Some((store, dep))
    } else {
        None
    };

    if cmd.run {
        pb.set_message("Running the synthesized workflow...");
        let input = cmd
            .input
            .map_or(Ok(serde_json::Value::Null), |i| serde_json::from_str(&i))
            .context("--input is not valid JSON")?;
        // If we deployed, run through the deployment so the run is traced back to it.
        let output = match &deployment {
            Some((store, dep)) => gaussflow_synth::deploy::run_deployment(dep, input, store).await,
            None => gaussflow_synth::run(&result, input).await,
        }
        .map_err(|e| anyhow::anyhow!("Run failed: {e}"))?;
        println!("\n{}", style("Run result").bold().underlined());
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        println!(
            "\n{}",
            style("Re-run with --run to execute (and --deploy to persist) this workflow.").dim()
        );
    }

    Ok(())
}

pub async fn manage_templates(cmd: TemplateCommand, _state: AppState) -> Result<()> {
    match cmd.subcommand {
        TemplateSubcommand::List { tags: _, format: _ } => {
            println!("Listing templates...");
            // TODO: Implement template listing
        }
        TemplateSubcommand::Create {
            name,
            description: _,
            file: _,
            tags: _,
            version: _,
        } => {
            println!("Creating template: {}", name);
            // TODO: Implement template creation
        }
    }
    Ok(())
}

pub async fn monitor_workflows(cmd: MonitorCommand, _state: AppState) -> Result<()> {
    match cmd.subcommand {
        MonitorSubcommand::List {
            detailed: _,
            interval: _,
        } => {
            println!("Monitoring workflows...");
            // TODO: Implement workflow monitoring
        }
        MonitorSubcommand::Show { id } => {
            println!("Showing workflow: {}", id);
            // TODO: Implement workflow details
        }
    }
    Ok(())
}

pub async fn manage_history(cmd: HistoryCommand, _state: AppState) -> Result<()> {
    match cmd.subcommand {
        HistorySubcommand::List {
            workflow_id: _,
            status: _,
            limit: _,
            format: _,
        } => {
            println!("Listing workflow history...");
            // TODO: Implement history listing
        }
    }
    Ok(())
}

pub async fn manage_config(cmd: ConfigCommand, _state: AppState) -> Result<()> {
    match cmd.subcommand {
        ConfigSubcommand::Show {
            sensitive: _,
            format: _,
        } => {
            println!("Showing configuration...");
            // TODO: Implement config display
        }
        ConfigSubcommand::Set { key, value } => {
            println!("Setting config: {} = {}", key, value);
            // TODO: Implement config setting
        }
    }
    Ok(())
}

pub async fn start_server(cmd: ServeCommand, _state: AppState) -> Result<()> {
    println!("Starting GaussFlow server on {}:{}", cmd.host, cmd.port);

    // Set environment variables for the server
    std::env::set_var("GF_PORT", cmd.port.to_string());
    std::env::set_var("GF_HOST", cmd.host.clone());

    if cmd.cors {
        std::env::set_var("GF_CORS_ENABLED", "true");
    }

    if cmd.auth {
        std::env::set_var("GF_AUTH_ENABLED", "true");
    }

    if cmd.metrics {
        std::env::set_var("GF_METRICS_ENABLED", "true");
    }

    if cmd.health {
        std::env::set_var("GF_HEALTH_ENABLED", "true");
    }

    // Start the server
    crate::server::start_server().await?;

    Ok(())
}

pub async fn run_benchmarks(cmd: BenchCommand, _state: AppState) -> Result<()> {
    println!(
        "Running benchmarks for workflow: {}",
        cmd.workflow_file.display()
    );

    // Implementation for benchmarking
    // TODO: Implement comprehensive benchmarking

    println!("Benchmark completed");
    Ok(())
}

pub async fn manage_database(cmd: DbCommand, _state: AppState) -> Result<()> {
    match cmd.subcommand {
        DbSubcommand::Init { force: _ } => {
            println!("Initializing database...");
            // TODO: Implement database initialization
        }
        DbSubcommand::Status { detailed: _ } => {
            println!("Checking database status...");
            // TODO: Implement database status
        }
    }
    Ok(())
}

pub async fn manage_cache(cmd: CacheCommand, state: AppState) -> Result<()> {
    match cmd.subcommand {
        CacheSubcommand::Stats { detailed } => {
            let stats = state.cache.stats().await;
            if detailed {
                println!("Cache Statistics:");
                println!("  Hits: {}", stats.hits);
                println!("  Misses: {}", stats.misses);
                println!(
                    "  Hit Rate: {:.2}%",
                    if stats.hits + stats.misses > 0 {
                        (stats.hits as f64 / (stats.hits + stats.misses) as f64) * 100.0
                    } else {
                        0.0
                    }
                );
                println!("  Size: {}", stats.size);
                println!("  Memory Usage: {} bytes", stats.memory_usage);
                println!("  Evictions: {}", stats.evictions);
            } else {
                println!(
                    "Cache: {} hits, {} misses, {} entries",
                    stats.hits, stats.misses, stats.size
                );
            }
        }
        CacheSubcommand::Clear { layer: _ } => {
            state.cache.clear().await?;
            println!("Cache cleared successfully");
        }
    }
    Ok(())
}

// Helper functions

async fn validate_workflow_strict(dag: &gaussflow_core::TypeSafeDag) -> Result<()> {
    // Perform strict validation checks
    // TODO: Implement comprehensive validation

    // Check for cycles
    // Check for cycles using petgraph's algorithms
    if petgraph::algo::is_cyclic_directed(&dag.graph) {
        anyhow::bail!("Workflow contains cycles");
    }

    // Check for disconnected nodes
    if dag.graph.node_count() > 0 && dag.graph.edge_count() == 0 {
        warn!("Workflow has no edges - all nodes are disconnected");
    }

    Ok(())
}
