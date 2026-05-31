use anyhow::Result;
use config::{Config, Environment, File};
use notify::Watcher;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Application configuration with advanced features
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AppConfig {
    /// Database configuration
    pub database: DatabaseConfig,

    /// Cache configuration
    pub cache: CacheConfig,

    /// Server configuration
    pub server: ServerConfig,

    /// Metrics configuration
    pub metrics: MetricsConfig,

    /// Security configuration
    pub security: SecurityConfig,

    /// Performance configuration
    pub performance: PerformanceConfig,

    /// Logging configuration
    pub logging: LoggingConfig,

    /// External service configurations
    pub services: ServicesConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    pub url: String,
    pub namespace: String,
    pub database: String,
    pub username: String,
    pub password: String,
    pub max_connections: usize,
    pub connection_timeout: u64,
    pub query_timeout: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    pub enabled: bool,
    pub max_size: usize,
    pub ttl_seconds: u64,
    pub eviction_policy: String,
    pub compression: bool,
    pub persistence: bool,
    pub persistence_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub workers: usize,
    pub max_connections: usize,
    pub timeout: u64,
    pub cors_enabled: bool,
    pub cors_origins: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsConfig {
    pub enabled: bool,
    pub port: u16,
    pub path: String,
    pub retention_days: u32,
    pub export_interval: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityConfig {
    pub jwt_secret: String,
    pub jwt_expiration: u64,
    pub rate_limit_enabled: bool,
    pub rate_limit_requests: u32,
    pub rate_limit_window: u64,
    pub encryption_enabled: bool,
    pub encryption_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    pub max_concurrent_workflows: usize,
    pub workflow_timeout: u64,
    pub memory_limit_mb: usize,
    pub cpu_limit_percent: u8,
    pub enable_profiling: bool,
    pub profiling_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    pub level: String,
    pub format: String,
    pub output: String,
    pub file_path: Option<PathBuf>,
    pub max_file_size: usize,
    pub max_files: usize,
    pub enable_structured_logging: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ServicesConfig {
    pub openai: OpenAIConfig,
    pub anthropic: AnthropicConfig,
    pub azure: AzureConfig,
    pub aws: AWSConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenAIConfig {
    pub api_key: Option<String>,
    pub base_url: String,
    pub timeout: u64,
    pub max_retries: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnthropicConfig {
    pub api_key: Option<String>,
    pub base_url: String,
    pub timeout: u64,
    pub max_retries: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AzureConfig {
    pub endpoint: Option<String>,
    pub api_key: Option<String>,
    pub deployment_name: Option<String>,
    pub timeout: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AWSConfig {
    pub region: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            url: "http://127.0.0.1:8000".to_string(),
            namespace: "gaussflow".to_string(),
            database: "gaussflow".to_string(),
            username: std::env::var("GAUSSFLOW_DB_USER").unwrap_or_else(|_| "root".to_string()),
            // No hardcoded secret: sourced from the environment, with a local-dev fallback.
            password: std::env::var("GAUSSFLOW_DB_PASS").unwrap_or_else(|_| "root".to_string()),
            max_connections: 10,
            connection_timeout: 30,
            query_timeout: 60,
        }
    }
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_size: 1000,
            ttl_seconds: 3600,
            eviction_policy: "lru".to_string(),
            compression: true,
            persistence: false,
            persistence_path: None,
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 3030,
            workers: num_cpus::get(),
            max_connections: 1000,
            timeout: 30,
            cors_enabled: true,
            cors_origins: vec!["*".to_string()],
        }
    }
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            port: 9090,
            path: "/metrics".to_string(),
            retention_days: 30,
            export_interval: 60,
        }
    }
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            // No hardcoded secret. Sourced from the environment; empty by default so that
            // `Config::validate()` rejects an unconfigured secret rather than shipping a known one.
            jwt_secret: std::env::var("GAUSSFLOW_JWT_SECRET").unwrap_or_default(),
            jwt_expiration: 3600,
            rate_limit_enabled: true,
            rate_limit_requests: 100,
            rate_limit_window: 60,
            encryption_enabled: false,
            encryption_key: None,
        }
    }
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            max_concurrent_workflows: 100,
            workflow_timeout: 3600,
            memory_limit_mb: 1024,
            cpu_limit_percent: 80,
            enable_profiling: false,
            profiling_path: None,
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            format: "json".to_string(),
            output: "stdout".to_string(),
            file_path: None,
            max_file_size: 100 * 1024 * 1024, // 100MB
            max_files: 5,
            enable_structured_logging: true,
        }
    }
}

impl Default for OpenAIConfig {
    fn default() -> Self {
        Self {
            api_key: None,
            base_url: "https://api.openai.com/v1".to_string(),
            timeout: 30,
            max_retries: 3,
        }
    }
}

impl Default for AnthropicConfig {
    fn default() -> Self {
        Self {
            api_key: None,
            base_url: "https://api.anthropic.com".to_string(),
            timeout: 30,
            max_retries: 3,
        }
    }
}

impl Default for AzureConfig {
    fn default() -> Self {
        Self {
            endpoint: None,
            api_key: None,
            deployment_name: None,
            timeout: 30,
        }
    }
}

/// Advanced configuration manager with hot reloading and validation
pub struct ConfigManager {
    config: Arc<RwLock<AppConfig>>,
    config_path: Option<PathBuf>,
    watcher: Option<notify::RecommendedWatcher>,
}

impl ConfigManager {
    /// Create a new configuration manager
    pub async fn new(config_path: Option<PathBuf>) -> Result<Self> {
        let config = Self::load_config(&config_path).await?;
        let config = Arc::new(RwLock::new(config));

        let mut manager = Self {
            config,
            config_path,
            watcher: None,
        };

        // Set up hot reloading if config file exists
        if let Some(path) = &manager.config_path {
            if path.exists() {
                manager.setup_watcher().await?;
            }
        }

        Ok(manager)
    }

    /// Load configuration from file and environment
    async fn load_config(config_path: &Option<PathBuf>) -> Result<AppConfig> {
        let mut builder = Config::builder();

        // Add default configuration
        builder = builder.add_source(File::from_str(
            include_str!("../config/default.toml"),
            config::FileFormat::Toml,
        ));

        // Add user configuration file if specified
        if let Some(path) = config_path {
            if path.exists() {
                info!("Loading configuration from: {}", path.display());
                builder = builder.add_source(File::from(path.as_path()));
            } else {
                warn!("Configuration file not found: {}", path.display());
            }
        } else {
            // Try to load from default locations
            let default_paths = vec![
                PathBuf::from("config.toml"),
                dirs::config_dir()
                    .unwrap_or_else(|| PathBuf::from("."))
                    .join("gaussflow/config.toml"),
                dirs::home_dir()
                    .unwrap_or_else(|| PathBuf::from("."))
                    .join(".gaussflow/config.toml"),
            ];

            for path in default_paths {
                if path.exists() {
                    info!("Loading configuration from: {}", path.display());
                    builder = builder.add_source(File::from(path));
                    break;
                }
            }
        }

        // Add environment variables
        builder = builder.add_source(
            Environment::with_prefix("GAUSSFLOW")
                .separator("_")
                .list_separator(",")
                .try_parsing(true),
        );

        // Build and deserialize configuration
        let config = builder.build()?;
        let app_config: AppConfig = config.try_deserialize()?;

        // Validate configuration
        app_config.validate()?;

        // Set environment variables for backward compatibility
        Self::set_env_vars(&app_config);

        Ok(app_config)
    }

    /// Validate configuration
    fn validate(&self) -> Result<()> {
        // Add validation logic here
        Ok(())
    }

    /// Set environment variables for backward compatibility
    fn set_env_vars(config: &AppConfig) {
        if let Some(api_key) = &config.services.openai.api_key {
            std::env::set_var("OPENAI_API_KEY", api_key);
        }
        if let Some(api_key) = &config.services.anthropic.api_key {
            std::env::set_var("ANTHROPIC_API_KEY", api_key);
        }
        std::env::set_var("GF_DB_URL", &config.database.url);
        std::env::set_var("GF_PORT", config.server.port.to_string());
    }

    /// Set up file watcher for hot reloading
    async fn setup_watcher(&mut self) -> Result<()> {
        if let Some(path) = &self.config_path {
            let (tx, mut rx) = tokio::sync::mpsc::channel(100);

            let mut watcher = notify::recommended_watcher(move |res| {
                if let Ok(event) = res {
                    let _ = tx.blocking_send(event);
                }
            })?;

            watcher.watch(path, notify::RecursiveMode::NonRecursive)?;

            // Spawn watcher task
            let config = self.config.clone();
            let config_path = path.clone();

            tokio::spawn(async move {
                while let Some(event) = rx.recv().await {
                    debug!("Configuration file changed: {:?}", event);

                    // Reload configuration
                    if let Ok(new_config) = Self::load_config(&Some(config_path.clone())).await {
                        let mut config = config.write().await;
                        *config = new_config;
                        info!("Configuration reloaded successfully");
                    } else {
                        warn!("Failed to reload configuration");
                    }
                }
            });

            self.watcher = Some(watcher);
        }

        Ok(())
    }

    /// Get current configuration
    pub async fn get(&self) -> AppConfig {
        self.config.read().await.clone()
    }

    /// Update configuration
    pub async fn update(&self, new_config: AppConfig) -> Result<()> {
        new_config.validate()?;
        let mut config = self.config.write().await;
        *config = new_config;
        Self::set_env_vars(&config);
        Ok(())
    }

    /// Get database configuration
    pub async fn database(&self) -> DatabaseConfig {
        self.config.read().await.database.clone()
    }

    /// Get cache configuration
    pub async fn cache(&self) -> CacheConfig {
        self.config.read().await.cache.clone()
    }

    /// Get server configuration
    pub async fn server(&self) -> ServerConfig {
        self.config.read().await.server.clone()
    }

    /// Get metrics configuration
    pub async fn metrics(&self) -> MetricsConfig {
        self.config.read().await.metrics.clone()
    }

    /// Get security configuration
    pub async fn security(&self) -> SecurityConfig {
        self.config.read().await.security.clone()
    }

    /// Get performance configuration
    pub async fn performance(&self) -> PerformanceConfig {
        self.config.read().await.performance.clone()
    }

    /// Get logging configuration
    pub async fn logging(&self) -> LoggingConfig {
        self.config.read().await.logging.clone()
    }

    /// Get services configuration
    pub async fn services(&self) -> ServicesConfig {
        self.config.read().await.services.clone()
    }
}

impl AppConfig {
    /// Validate configuration
    pub fn validate(&self) -> Result<()> {
        // Validate database configuration
        if self.database.url.is_empty() {
            anyhow::bail!("Database URL cannot be empty");
        }

        // Validate server configuration
        if self.server.port == 0 {
            anyhow::bail!("Server port cannot be 0");
        }

        // Validate security configuration
        if self.security.jwt_secret.is_empty() {
            anyhow::bail!("JWT secret cannot be empty");
        }

        // Validate performance configuration
        if self.performance.max_concurrent_workflows == 0 {
            anyhow::bail!("Max concurrent workflows cannot be 0");
        }

        Ok(())
    }
}
