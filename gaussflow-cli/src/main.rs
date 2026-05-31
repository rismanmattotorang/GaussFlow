use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use console::style;
use indicatif::{ProgressBar, ProgressStyle};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info, warn};
use tracing_subscriber::{fmt, EnvFilter};
use std::process::ExitCode;

mod cache;
mod commands;
mod config;
mod database;
mod metrics;
mod server;
mod utils;

use cache::CacheManager;
use commands::*;
use config::ConfigManager;
use database::DatabaseManager;
use metrics::MetricsManager;

/// GaussFlow: Advanced DAG Workflow Engine for Multi-LLM & Agent Pipelines
/// 
/// A high-performance, production-ready workflow engine with advanced features:
/// - Parallel execution with intelligent scheduling
/// - Real-time monitoring and metrics
/// - Distributed caching and state management
/// - Advanced error handling and recovery
/// - Comprehensive logging and tracing
#[derive(Parser, Debug)]
#[command(
    name = "gaussflow",
    author = "GaussFlow Team",
    version = env!("CARGO_PKG_VERSION"),
    about = "Advanced DAG Workflow Engine for Multi-LLM & Agent Pipelines",
    long_about = "GaussFlow is a high-performance workflow engine designed for complex multi-agent pipelines, featuring advanced scheduling, real-time monitoring, distributed caching, and comprehensive error handling."
)]
struct Cli {
    /// Enable verbose logging
    #[arg(short, long, global = true)]
    verbose: bool,

    /// Enable debug mode with additional logging
    #[arg(short, long, global = true)]
    debug: bool,

    /// Configuration file path
    #[arg(short, long, global = true)]
    config: Option<PathBuf>,

    /// Enable performance profiling
    #[arg(long, global = true)]
    profile: bool,

    /// Enable metrics collection
    #[arg(long, global = true, default_value = "true")]
    metrics: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Validate workflow specifications
    Validate(ValidateCommand),
    
    /// Execute workflows with advanced features
    Run(RunCommand),
    
    /// Manage workflow templates and libraries
    Template(TemplateCommand),
    
    /// Monitor and manage running workflows
    Monitor(MonitorCommand),
    
    /// Manage workflow history and artifacts
    History(HistoryCommand),
    
    /// Configure GaussFlow settings
    Config(ConfigCommand),
    
    /// Start the GaussFlow server
    Serve(ServeCommand),
    
    /// Performance testing and benchmarking
    Bench(BenchCommand),
    
    /// Database management operations
    Db(DbCommand),
    
    /// Cache management operations
    Cache(CacheCommand),
}

/// Global application state
#[derive(Clone)]
struct AppState {
    config: Arc<ConfigManager>,
    cache: Arc<CacheManager>,
    database: Arc<DatabaseManager>,
    metrics: Arc<MetricsManager>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Parse CLI arguments
    let cli = Cli::parse();
    
    // Initialize logging with advanced configuration
    init_logging(&cli)?;
    
    // Initialize application state
    let state = init_app_state(&cli).await?;
    
    // Initialize metrics if enabled
    if cli.metrics {
        // Note: init() requires mutable access, but we have Arc
        // We'll initialize metrics in the MetricsManager::new() instead
    }
    
    // Record startup metrics
    state.metrics.record_startup().await;
    
    info!("GaussFlow CLI initialized successfully");
    info!("Version: {}", env!("CARGO_PKG_VERSION"));
    info!("Debug mode: {}", cli.debug);
    info!("Metrics enabled: {}", cli.metrics);
    
    // Execute command with enhanced error handling
    let result = execute_command(cli.command, state).await;
    
    match result {
        Ok(_) => {
            info!("Command completed successfully");
            Ok(())
        }
        Err(e) => {
            // Enhanced error handling with proper exit codes and user-friendly messages
            let exit_code = handle_cli_error(&e);
            error!("Command failed: {}", e);
            display_error(&e);
            std::process::exit(exit_code);
        }
    }
}

fn init_logging(cli: &Cli) -> Result<()> {
    let log_level = if cli.debug {
        "debug"
    } else if cli.verbose {
        "info"
    } else {
        "warn"
    };
    
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(log_level));
    
    // Configure structured logging with JSON output
    let subscriber = fmt::Subscriber::builder()
        .with_env_filter(env_filter)
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_target(false)
        .with_ansi(false)
        .json()
        .finish();
    
    tracing::subscriber::set_global_default(subscriber)
        .context("Failed to set tracing subscriber")?;
    
    Ok(())
}

async fn init_app_state(cli: &Cli) -> Result<AppState> {
    // Initialize configuration manager
    let config = Arc::new(ConfigManager::new(cli.config.clone()).await?);
    
    // Initialize cache manager with advanced features
    let cache = Arc::new(CacheManager::new(&config.get().await.cache).await?);
    
    // Initialize database manager
    let database = Arc::new(DatabaseManager::new(&config.get().await.database).await?);
    
    // Initialize metrics manager
    let metrics = Arc::new(MetricsManager::new(&config.get().await.metrics).await?);
    
    Ok(AppState {
        config,
        cache,
        database,
        metrics,
    })
}

async fn execute_command(command: Commands, state: AppState) -> Result<()> {
    match command {
        Commands::Validate(cmd) => {
            let pb = create_progress_bar("Validating workflow");
            let result = validate_workflow(cmd, state, pb.clone()).await;
            pb.finish_with_message("Validation completed");
            result
        }
        
        Commands::Run(cmd) => {
            let pb = create_progress_bar("Executing workflow");
            let result = run_workflow(cmd, state, pb.clone()).await;
            pb.finish_with_message("Execution completed");
            result
        }
        
        Commands::Template(cmd) => {
            manage_templates(cmd, state).await
        }
        
        Commands::Monitor(cmd) => {
            monitor_workflows(cmd, state).await
        }
        
        Commands::History(cmd) => {
            manage_history(cmd, state).await
        }
        
        Commands::Config(cmd) => {
            manage_config(cmd, state).await
        }
        
        Commands::Serve(cmd) => {
            start_server(cmd, state).await
        }
        
        Commands::Bench(cmd) => {
            run_benchmarks(cmd, state).await
        }
        
        Commands::Db(cmd) => {
            manage_database(cmd, state).await
        }
        
        Commands::Cache(cmd) => {
            manage_cache(cmd, state).await
        }
    }
}

fn create_progress_bar(message: &str) -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} {wide_msg}")
            .unwrap()
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"),
    );
    pb.set_message(message.to_string());
    pb
}

// Re-export command modules for easy access
pub use commands::*;

/// Enhanced error handling for CLI operations
fn handle_cli_error(error: &anyhow::Error) -> i32 {
    // Determine exit code based on error type
    let error_string = error.to_string().to_lowercase();
    
    if error_string.contains("timeout") || error_string.contains("timed out") {
        124 // SIGTERM timeout
    } else if error_string.contains("permission") || error_string.contains("access denied") {
        126 // Permission denied
    } else if error_string.contains("not found") || error_string.contains("no such file") {
        127 // Command not found
    } else if error_string.contains("invalid") || error_string.contains("malformed") {
        128 // Invalid argument
    } else if error_string.contains("cancelled") || error_string.contains("interrupted") {
        130 // SIGINT (Ctrl+C)
    } else if error_string.contains("authentication") || error_string.contains("unauthorized") {
        1 // General error
    } else {
        1 // Default exit code
    }
}

/// Display error with enhanced formatting and user-friendly messages
fn display_error(error: &anyhow::Error) {
    let error_string = error.to_string();
    
    // Determine error type and provide appropriate user message
    let (severity_icon, user_message) = if error_string.contains("timeout") {
        ("⏰", "Operation timed out. Please try again.")
    } else if error_string.contains("permission") {
        ("🔒", "Permission denied. Please check file permissions.")
    } else if error_string.contains("not found") {
        ("🔍", "File or resource not found. Please check the path.")
    } else if error_string.contains("invalid") {
        ("⚠️", "Invalid input. Please check your parameters.")
    } else if error_string.contains("authentication") {
        ("🔐", "Authentication failed. Please check your credentials.")
    } else if error_string.contains("network") || error_string.contains("connection") {
        ("🌐", "Network error. Please check your internet connection.")
    } else if error_string.contains("workflow") {
        ("⚙️", "Workflow error. Please check the workflow definition.")
    } else {
        ("❌", "An unexpected error occurred. Please try again.")
    };

    // Display error with proper formatting
    eprintln!("{} {}", style(severity_icon).red().bold(), style(user_message).red());
    
    // Show detailed error in debug mode or if verbose
    if std::env::var("RUST_LOG").unwrap_or_default().contains("debug") {
        eprintln!("{} {}", style("Details:").dim(), error_string);
    }
    
    // Provide helpful suggestions
    if error_string.contains("configuration") {
        eprintln!("{} Run 'gaussflow config show' to view current configuration", style("💡").blue());
    } else if error_string.contains("workflow") {
        eprintln!("{} Run 'gaussflow validate <workflow>' to check workflow syntax", style("💡").blue());
    } else if error_string.contains("network") {
        eprintln!("{} Check your internet connection and firewall settings", style("💡").blue());
    }
}
