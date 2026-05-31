//! GaussFlow TUI - Modern Terminal User Interface
//!
//! A beautiful and intuitive terminal interface for GaussFlow workflow management,
//! featuring real-time execution monitoring, logs viewing, and system metrics.

use std::io::stdout;
use std::time::{Duration, Instant};

use anyhow::Result;
use clap::Parser;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Alignment, Constraint, Direction, Layout, Margin, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, List, ListItem, ListState, Paragraph, Tabs, Wrap},
    Frame, Terminal,
};
use serde::{Deserialize, Serialize};

// ============================================================================
// CLI Arguments
// ============================================================================

#[derive(Parser, Debug)]
#[command(name = "gaussflow-tui")]
#[command(author = "GaussFlow Team")]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(about = "Terminal UI for GaussFlow DAG Workflow Engine")]
struct Args {
    /// API server URL
    #[arg(short, long, default_value = "http://localhost:8080")]
    api_url: String,

    /// Refresh interval in milliseconds
    #[arg(short, long, default_value = "1000")]
    refresh: u64,

    /// Show debug information
    #[arg(short, long, default_value = "false")]
    debug: bool,
}

// ============================================================================
// Data Models
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WorkflowInfo {
    id: String,
    name: String,
    description: Option<String>,
    node_count: usize,
    edge_count: usize,
    status: String,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ExecutionInfo {
    id: String,
    workflow_id: String,
    workflow_name: String,
    status: String,
    started_at: String,
    finished_at: Option<String>,
    current_node: Option<String>,
    progress: f32,
    error: Option<String>,
    #[serde(default)]
    completed_nodes: Vec<String>,
    #[serde(default)]
    failed_nodes: Vec<String>,
    #[serde(default)]
    duration_ms: Option<u64>,
    #[serde(default)]
    logs: Vec<LogEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LogEntry {
    timestamp: String,
    level: String,
    message: String,
    node_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SystemStats {
    total_workflows: usize,
    active_executions: usize,
    completed_executions: usize,
    failed_executions: usize,
    success_rate: f64,
    uptime_seconds: u64,
}

impl Default for SystemStats {
    fn default() -> Self {
        Self {
            total_workflows: 0,
            active_executions: 0,
            completed_executions: 0,
            failed_executions: 0,
            success_rate: 0.0,
            uptime_seconds: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ApiResponse<T> {
    success: bool,
    data: Option<T>,
    error: Option<ApiError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ApiError {
    code: String,
    message: String,
}

// ============================================================================
// Application State
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq)]
enum Tab {
    Dashboard,
    Workflows,
    Executions,
    Logs,
    Help,
}

impl Tab {
    fn all() -> Vec<Tab> {
        vec![
            Tab::Dashboard,
            Tab::Workflows,
            Tab::Executions,
            Tab::Logs,
            Tab::Help,
        ]
    }

    fn title(&self) -> &'static str {
        match self {
            Tab::Dashboard => " Dashboard ",
            Tab::Workflows => " Workflows ",
            Tab::Executions => " Executions ",
            Tab::Logs => " Logs ",
            Tab::Help => " Help ",
        }
    }

    fn index(&self) -> usize {
        match self {
            Tab::Dashboard => 0,
            Tab::Workflows => 1,
            Tab::Executions => 2,
            Tab::Logs => 3,
            Tab::Help => 4,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum DialogType {
    None,
    Execute,
    Delete,
    Cancel,
    Error,
}

struct App {
    api_url: String,
    current_tab: Tab,
    should_quit: bool,
    debug: bool,

    // Data
    workflows: Vec<WorkflowInfo>,
    executions: Vec<ExecutionInfo>,
    logs: Vec<LogEntry>,
    stats: SystemStats,

    // UI State
    workflow_list_state: ListState,
    execution_list_state: ListState,
    log_scroll: u16,

    // Status
    last_refresh: Instant,
    refresh_interval: Duration,
    connection_status: bool,
    status_message: String,
    error_message: Option<String>,

    // Dialog state
    dialog_type: DialogType,
    selected_workflow_id: Option<String>,
    selected_execution_id: Option<String>,
}

impl App {
    fn new(api_url: String, refresh_interval: Duration, debug: bool) -> Self {
        Self {
            api_url,
            current_tab: Tab::Dashboard,
            should_quit: false,
            debug,
            workflows: Vec::new(),
            executions: Vec::new(),
            logs: Vec::new(),
            stats: SystemStats::default(),
            workflow_list_state: ListState::default(),
            execution_list_state: ListState::default(),
            log_scroll: 0,
            last_refresh: Instant::now(),
            refresh_interval,
            connection_status: false,
            status_message: "Connecting...".to_string(),
            error_message: None,
            dialog_type: DialogType::None,
            selected_workflow_id: None,
            selected_execution_id: None,
        }
    }

    fn next_tab(&mut self) {
        let tabs = Tab::all();
        let current_idx = self.current_tab.index();
        self.current_tab = tabs[(current_idx + 1) % tabs.len()];
    }

    fn prev_tab(&mut self) {
        let tabs = Tab::all();
        let current_idx = self.current_tab.index();
        self.current_tab = tabs[(current_idx + tabs.len() - 1) % tabs.len()];
    }

    fn next_item(&mut self) {
        match self.current_tab {
            Tab::Workflows => {
                if !self.workflows.is_empty() {
                    let i = match self.workflow_list_state.selected() {
                        Some(i) => (i + 1) % self.workflows.len(),
                        None => 0,
                    };
                    self.workflow_list_state.select(Some(i));
                }
            }
            Tab::Executions => {
                if !self.executions.is_empty() {
                    let i = match self.execution_list_state.selected() {
                        Some(i) => (i + 1) % self.executions.len(),
                        None => 0,
                    };
                    self.execution_list_state.select(Some(i));
                }
            }
            Tab::Logs => {
                if self.log_scroll < self.logs.len().saturating_sub(1) as u16 {
                    self.log_scroll += 1;
                }
            }
            _ => {}
        }
    }

    fn prev_item(&mut self) {
        match self.current_tab {
            Tab::Workflows => {
                if !self.workflows.is_empty() {
                    let i = match self.workflow_list_state.selected() {
                        Some(i) => (i + self.workflows.len() - 1) % self.workflows.len(),
                        None => 0,
                    };
                    self.workflow_list_state.select(Some(i));
                }
            }
            Tab::Executions => {
                if !self.executions.is_empty() {
                    let i = match self.execution_list_state.selected() {
                        Some(i) => (i + self.executions.len() - 1) % self.executions.len(),
                        None => 0,
                    };
                    self.execution_list_state.select(Some(i));
                }
            }
            Tab::Logs => {
                self.log_scroll = self.log_scroll.saturating_sub(1);
            }
            _ => {}
        }
    }

    fn select_item(&mut self) {
        if self.current_tab == Tab::Workflows {
            if let Some(idx) = self.workflow_list_state.selected() {
                if let Some(workflow) = self.workflows.get(idx) {
                    self.selected_workflow_id = Some(workflow.id.clone());
                    self.dialog_type = DialogType::Execute;
                }
            }
        }
    }

    fn show_delete_dialog(&mut self) {
        if self.current_tab == Tab::Workflows {
            if let Some(idx) = self.workflow_list_state.selected() {
                if let Some(workflow) = self.workflows.get(idx) {
                    self.selected_workflow_id = Some(workflow.id.clone());
                    self.dialog_type = DialogType::Delete;
                }
            }
        }
    }

    fn show_cancel_dialog(&mut self) {
        if self.current_tab == Tab::Executions {
            if let Some(idx) = self.execution_list_state.selected() {
                if let Some(exec) = self.executions.get(idx) {
                    if exec.status == "running" {
                        self.selected_execution_id = Some(exec.id.clone());
                        self.dialog_type = DialogType::Cancel;
                    } else {
                        self.status_message =
                            "Only running executions can be cancelled".to_string();
                    }
                }
            }
        }
    }

    async fn refresh_data(&mut self) {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap_or_default();

        // Fetch workflows
        match client
            .get(format!("{}/api/workflows", self.api_url))
            .send()
            .await
        {
            Ok(response) => {
                if let Ok(data) = response.json::<ApiResponse<Vec<WorkflowInfo>>>().await {
                    if data.success {
                        self.workflows = data.data.unwrap_or_default();
                        self.connection_status = true;
                        self.error_message = None;
                    }
                }
            }
            Err(e) => {
                self.connection_status = false;
                self.error_message = Some(format!("Connection error: {}", e));
            }
        }

        // Fetch executions
        if let Ok(response) = client
            .get(format!("{}/api/executions", self.api_url))
            .send()
            .await
        {
            if let Ok(data) = response.json::<ApiResponse<Vec<ExecutionInfo>>>().await {
                if data.success {
                    self.executions = data.data.unwrap_or_default();
                    // Update logs from executions
                    self.logs = self
                        .executions
                        .iter()
                        .flat_map(|e| e.logs.clone())
                        .take(100)
                        .collect();
                }
            }
        }

        // Fetch stats
        if let Ok(response) = client
            .get(format!("{}/api/stats", self.api_url))
            .send()
            .await
        {
            if let Ok(data) = response.json::<ApiResponse<serde_json::Value>>().await {
                if data.success {
                    if let Some(stats_data) = data.data {
                        self.stats.total_workflows =
                            stats_data["workflows"]["total"].as_u64().unwrap_or(0) as usize;
                        self.stats.active_executions =
                            stats_data["executions"]["running"].as_u64().unwrap_or(0) as usize;
                        self.stats.completed_executions =
                            stats_data["executions"]["completed"].as_u64().unwrap_or(0) as usize;
                        self.stats.failed_executions =
                            stats_data["executions"]["failed"].as_u64().unwrap_or(0) as usize;
                        self.stats.success_rate = stats_data["executions"]["success_rate"]
                            .as_f64()
                            .unwrap_or(0.0);
                        self.stats.uptime_seconds =
                            stats_data["system"]["uptime_seconds"].as_u64().unwrap_or(0);
                    }
                }
            }
        }

        self.status_message = format!("Updated: {}", chrono::Local::now().format("%H:%M:%S"));
        self.last_refresh = Instant::now();
    }

    async fn execute_selected_workflow(&mut self) {
        if let Some(workflow_id) = &self.selected_workflow_id {
            let client = reqwest::Client::new();

            match client
                .post(format!(
                    "{}/api/workflows/{}/execute",
                    self.api_url, workflow_id
                ))
                .json(&serde_json::json!({ "input": null }))
                .send()
                .await
            {
                Ok(response) => {
                    if response.status().is_success() {
                        self.status_message = "✓ Execution started!".to_string();
                    } else {
                        if let Ok(data) = response.json::<ApiResponse<()>>().await {
                            self.status_message = format!(
                                "✗ {}",
                                data.error
                                    .map(|e| e.message)
                                    .unwrap_or("Failed".to_string())
                            );
                        }
                    }
                }
                Err(e) => {
                    self.status_message = format!("✗ Error: {}", e);
                }
            }
        }

        self.dialog_type = DialogType::None;
        self.selected_workflow_id = None;
    }

    async fn delete_selected_workflow(&mut self) {
        if let Some(workflow_id) = &self.selected_workflow_id {
            let client = reqwest::Client::new();

            match client
                .delete(format!("{}/api/workflows/{}", self.api_url, workflow_id))
                .send()
                .await
            {
                Ok(response) => {
                    if response.status().is_success() {
                        self.status_message = "✓ Workflow deleted".to_string();
                        self.workflows.retain(|w| w.id != *workflow_id);
                    } else {
                        if let Ok(data) = response.json::<ApiResponse<()>>().await {
                            self.status_message = format!(
                                "✗ {}",
                                data.error
                                    .map(|e| e.message)
                                    .unwrap_or("Failed".to_string())
                            );
                        }
                    }
                }
                Err(e) => {
                    self.status_message = format!("✗ Error: {}", e);
                }
            }
        }

        self.dialog_type = DialogType::None;
        self.selected_workflow_id = None;
    }

    async fn cancel_selected_execution(&mut self) {
        if let Some(exec_id) = &self.selected_execution_id {
            let client = reqwest::Client::new();

            match client
                .post(format!(
                    "{}/api/executions/{}/cancel",
                    self.api_url, exec_id
                ))
                .send()
                .await
            {
                Ok(response) => {
                    if response.status().is_success() {
                        self.status_message = "✓ Execution cancelled".to_string();
                    } else {
                        if let Ok(data) = response.json::<ApiResponse<()>>().await {
                            self.status_message = format!(
                                "✗ {}",
                                data.error
                                    .map(|e| e.message)
                                    .unwrap_or("Failed".to_string())
                            );
                        }
                    }
                }
                Err(e) => {
                    self.status_message = format!("✗ Error: {}", e);
                }
            }
        }

        self.dialog_type = DialogType::None;
        self.selected_execution_id = None;
    }
}

// ============================================================================
// UI Rendering
// ============================================================================

fn ui(frame: &mut Frame, app: &mut App) {
    let size = frame.area();

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(0),
            Constraint::Length(3),
        ])
        .split(size);

    render_header(frame, app, chunks[0]);

    match app.current_tab {
        Tab::Dashboard => render_dashboard(frame, app, chunks[1]),
        Tab::Workflows => render_workflows(frame, app, chunks[1]),
        Tab::Executions => render_executions(frame, app, chunks[1]),
        Tab::Logs => render_logs(frame, app, chunks[1]),
        Tab::Help => render_help(frame, chunks[1]),
    }

    render_status_bar(frame, app, chunks[2]);

    // Render dialogs
    match app.dialog_type {
        DialogType::Execute => render_execute_dialog(frame, app),
        DialogType::Delete => render_delete_dialog(frame, app),
        DialogType::Cancel => render_cancel_dialog(frame, app),
        DialogType::Error => render_error_dialog(frame, app),
        DialogType::None => {}
    }
}

fn render_header(frame: &mut Frame, app: &App, area: Rect) {
    let titles: Vec<Line> = Tab::all()
        .iter()
        .enumerate()
        .map(|(i, t)| {
            let style = if *t == app.current_tab {
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::DarkGray)
            };
            Line::from(Span::styled(format!("[{}]{}", i + 1, t.title()), style))
        })
        .collect();

    let tabs = Tabs::new(titles)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::DarkGray))
                .title(Span::styled(
                    " ∞ GaussFlow TUI ",
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                )),
        )
        .highlight_style(Style::default().fg(Color::Cyan))
        .select(app.current_tab.index());

    frame.render_widget(tabs, area);
}

fn render_dashboard(frame: &mut Frame, app: &App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(7), Constraint::Min(0)])
        .margin(1)
        .split(area);

    let stats_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(20),
            Constraint::Percentage(20),
            Constraint::Percentage(20),
            Constraint::Percentage(20),
            Constraint::Percentage(20),
        ])
        .split(chunks[0]);

    render_stat_card(
        frame,
        "Workflows",
        &app.stats.total_workflows.to_string(),
        Color::Cyan,
        stats_chunks[0],
    );
    render_stat_card(
        frame,
        "Running",
        &app.stats.active_executions.to_string(),
        Color::Yellow,
        stats_chunks[1],
    );
    render_stat_card(
        frame,
        "Completed",
        &app.stats.completed_executions.to_string(),
        Color::Green,
        stats_chunks[2],
    );
    render_stat_card(
        frame,
        "Failed",
        &app.stats.failed_executions.to_string(),
        Color::Red,
        stats_chunks[3],
    );
    render_stat_card(
        frame,
        "Success",
        &format!("{:.0}%", app.stats.success_rate),
        Color::Magenta,
        stats_chunks[4],
    );

    let activity_block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray))
        .title(Span::styled(
            " Recent Activity ",
            Style::default().fg(Color::White),
        ));

    let activity_items: Vec<ListItem> = app
        .executions
        .iter()
        .take(15)
        .map(|e| {
            let (status_color, status_icon) = match e.status.as_str() {
                "running" => (Color::Yellow, "⚡"),
                "completed" => (Color::Green, "✓"),
                "failed" => (Color::Red, "✗"),
                "cancelled" => (Color::DarkGray, "⊘"),
                _ => (Color::DarkGray, "○"),
            };

            ListItem::new(Line::from(vec![
                Span::styled(
                    format!("{} ", status_icon),
                    Style::default().fg(status_color),
                ),
                Span::styled(&e.workflow_name, Style::default().fg(Color::White)),
                Span::styled(" │ ", Style::default().fg(Color::DarkGray)),
                Span::styled(&e.status, Style::default().fg(status_color)),
                Span::styled(
                    format!(" ({:.0}%)", e.progress),
                    Style::default().fg(Color::DarkGray),
                ),
            ]))
        })
        .collect();

    let activity_list = List::new(activity_items)
        .block(activity_block)
        .highlight_style(Style::default().bg(Color::DarkGray));

    frame.render_widget(activity_list, chunks[1]);
}

fn render_stat_card(frame: &mut Frame, title: &str, value: &str, color: Color, area: Rect) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray))
        .title(Span::styled(
            format!(" {} ", title),
            Style::default().fg(color),
        ));

    let inner = block.inner(area);
    frame.render_widget(block, area);

    let text = Paragraph::new(value)
        .style(Style::default().fg(color).add_modifier(Modifier::BOLD))
        .alignment(Alignment::Center);

    let centered_area = Rect {
        x: inner.x,
        y: inner.y + inner.height / 2 - 1,
        width: inner.width,
        height: 2,
    };

    frame.render_widget(text, centered_area);
}

fn render_workflows(frame: &mut Frame, app: &mut App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .margin(1)
        .split(area);

    let workflow_items: Vec<ListItem> = app
        .workflows
        .iter()
        .map(|w| {
            let status_color = match w.status.as_str() {
                "active" => Color::Green,
                "draft" => Color::Yellow,
                "paused" => Color::Magenta,
                _ => Color::DarkGray,
            };

            ListItem::new(Line::from(vec![
                Span::styled("● ", Style::default().fg(status_color)),
                Span::styled(&w.name, Style::default().fg(Color::White)),
                Span::styled(
                    format!(" ({} nodes)", w.node_count),
                    Style::default().fg(Color::DarkGray),
                ),
            ]))
        })
        .collect();

    let workflow_list = List::new(workflow_items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::DarkGray))
                .title(Span::styled(
                    " Workflows ",
                    Style::default().fg(Color::White),
                )),
        )
        .highlight_style(Style::default().bg(Color::DarkGray).fg(Color::Cyan))
        .highlight_symbol("▸ ");

    frame.render_stateful_widget(workflow_list, chunks[0], &mut app.workflow_list_state);

    let details_block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray))
        .title(Span::styled(" Details ", Style::default().fg(Color::White)));

    let details_text = if let Some(idx) = app.workflow_list_state.selected() {
        if let Some(workflow) = app.workflows.get(idx) {
            let tags = if workflow.tags.is_empty() {
                "None".to_string()
            } else {
                workflow.tags.join(", ")
            };
            format!(
                "Name: {}\n\nDescription:\n{}\n\nStatus: {}\nNodes: {} │ Edges: {}\nVersion: {}\nTags: {}\n\n─────────────────────────\n[Enter] Execute  [d] Delete\n[r] Refresh",
                workflow.name,
                workflow.description.as_deref().unwrap_or("No description"),
                workflow.status,
                workflow.node_count,
                workflow.edge_count,
                workflow.version,
                tags
            )
        } else {
            "Select a workflow".to_string()
        }
    } else {
        "Select a workflow to view details\n\nUse ↑/↓ or j/k to navigate".to_string()
    };

    let details = Paragraph::new(details_text)
        .block(details_block)
        .style(Style::default().fg(Color::White))
        .wrap(Wrap { trim: true });

    frame.render_widget(details, chunks[1]);
}

fn render_executions(frame: &mut Frame, app: &mut App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
        .margin(1)
        .split(area);

    let execution_items: Vec<ListItem> = app
        .executions
        .iter()
        .map(|e| {
            let (status_color, status_icon) = match e.status.as_str() {
                "running" => (Color::Yellow, "⚡"),
                "completed" => (Color::Green, "✓"),
                "failed" => (Color::Red, "✗"),
                "cancelled" => (Color::DarkGray, "⊘"),
                "paused" => (Color::Magenta, "⏸"),
                _ => (Color::White, "○"),
            };

            let progress_bar = create_progress_bar(e.progress as f64 / 100.0, 15);

            ListItem::new(Line::from(vec![
                Span::styled(
                    format!("{} ", status_icon),
                    Style::default().fg(status_color),
                ),
                Span::styled(
                    format!("{:<20}", truncate(&e.workflow_name, 20)),
                    Style::default().fg(Color::White),
                ),
                Span::styled(" │ ", Style::default().fg(Color::DarkGray)),
                Span::styled(progress_bar, Style::default().fg(Color::Cyan)),
                Span::styled(
                    format!(" {:>3.0}%", e.progress),
                    Style::default().fg(Color::DarkGray),
                ),
            ]))
        })
        .collect();

    let execution_list = List::new(execution_items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::DarkGray))
                .title(Span::styled(
                    " Executions ",
                    Style::default().fg(Color::White),
                )),
        )
        .highlight_style(Style::default().bg(Color::DarkGray).fg(Color::Cyan))
        .highlight_symbol("▸ ");

    frame.render_stateful_widget(execution_list, chunks[0], &mut app.execution_list_state);

    let details_block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray))
        .title(Span::styled(
            " Execution Details ",
            Style::default().fg(Color::White),
        ));

    let details_text = if let Some(idx) = app.execution_list_state.selected() {
        if let Some(exec) = app.executions.get(idx) {
            let duration = exec
                .duration_ms
                .map(|d| format!("{:.2}s", d as f64 / 1000.0))
                .unwrap_or("-".to_string());
            format!(
                "Workflow: {}\nID: {}\n\nStatus: {} │ Progress: {:.0}%\nCurrent Node: {}\nCompleted: {} │ Failed: {}\nDuration: {}\n\n─────────────────────────\n{}",
                exec.workflow_name,
                &exec.id[..8.min(exec.id.len())],
                exec.status.to_uppercase(),
                exec.progress,
                exec.current_node.as_deref().unwrap_or("-"),
                exec.completed_nodes.len(),
                exec.failed_nodes.len(),
                duration,
                if exec.status == "running" { "[c] Cancel execution" } else { "" }
            )
        } else {
            "Select an execution".to_string()
        }
    } else {
        "Select an execution to view details".to_string()
    };

    let details = Paragraph::new(details_text)
        .block(details_block)
        .style(Style::default().fg(Color::White))
        .wrap(Wrap { trim: true });

    frame.render_widget(details, chunks[1]);
}

fn render_logs(frame: &mut Frame, app: &App, area: Rect) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray))
        .title(Span::styled(
            format!(" Logs ({}) ", app.logs.len()),
            Style::default().fg(Color::White),
        ));

    let log_items: Vec<ListItem> = app
        .logs
        .iter()
        .skip(app.log_scroll as usize)
        .map(|log| {
            let level_color = match log.level.to_uppercase().as_str() {
                "ERROR" => Color::Red,
                "WARN" => Color::Yellow,
                "INFO" => Color::Cyan,
                "DEBUG" => Color::DarkGray,
                _ => Color::White,
            };

            ListItem::new(Line::from(vec![
                Span::styled(
                    format!("{:5} ", log.level.to_uppercase()),
                    Style::default().fg(level_color),
                ),
                Span::styled(&log.message, Style::default().fg(Color::White)),
                if let Some(node) = &log.node_id {
                    Span::styled(format!(" [{}]", node), Style::default().fg(Color::DarkGray))
                } else {
                    Span::raw("")
                },
            ]))
        })
        .collect();

    let logs_list = List::new(log_items)
        .block(block)
        .highlight_style(Style::default().bg(Color::DarkGray));

    frame.render_widget(
        logs_list,
        area.inner(Margin {
            horizontal: 1,
            vertical: 1,
        }),
    );
}

fn render_help(frame: &mut Frame, area: Rect) {
    let help_text = r#"
  ╭─────────────────────────────────────────────────────────────────────╮
  │                      GaussFlow TUI - Help                           │
  ╰─────────────────────────────────────────────────────────────────────╯

  NAVIGATION
  ──────────────────────────────────────────────────────────────────────
  Tab / Shift+Tab       Switch between tabs
  1-5                   Jump to specific tab
  ↑/k  ↓/j              Navigate lists
  
  ACTIONS
  ──────────────────────────────────────────────────────────────────────
  Enter / x             Execute selected workflow
  d                     Delete selected workflow
  c                     Cancel selected execution (if running)
  r                     Refresh data
  
  GENERAL
  ──────────────────────────────────────────────────────────────────────
  q / Ctrl+C            Quit application
  ?                     Show this help
  Esc                   Close dialogs

  ──────────────────────────────────────────────────────────────────────
  
  GaussFlow - Advanced DAG Workflow Engine for Multi-LLM & Agent Pipelines
  Version: 0.0.1 │ https://github.com/gaussflow
  
  Press Esc or ? to close this help screen.
"#;

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan))
        .title(Span::styled(
            " Help ",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ));

    let help = Paragraph::new(help_text)
        .block(block)
        .style(Style::default().fg(Color::White))
        .alignment(Alignment::Left);

    frame.render_widget(
        help,
        area.inner(Margin {
            horizontal: 1,
            vertical: 1,
        }),
    );
}

fn render_status_bar(frame: &mut Frame, app: &App, area: Rect) {
    let status_color = if app.connection_status {
        Color::Green
    } else {
        Color::Red
    };

    let status_text = Line::from(vec![
        Span::styled(
            if app.connection_status {
                " ● Connected "
            } else {
                " ● Disconnected "
            },
            Style::default().fg(status_color),
        ),
        Span::styled("│", Style::default().fg(Color::DarkGray)),
        Span::styled(
            format!(" {} ", app.status_message),
            Style::default().fg(Color::DarkGray),
        ),
        Span::styled("│", Style::default().fg(Color::DarkGray)),
        Span::styled(
            " Tab:Switch  ↑↓:Nav  Enter:Select  d:Delete  c:Cancel  r:Refresh  q:Quit  ?:Help ",
            Style::default().fg(Color::DarkGray),
        ),
    ]);

    let status_bar = Paragraph::new(status_text).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray)),
    );

    frame.render_widget(status_bar, area);
}

fn render_execute_dialog(frame: &mut Frame, app: &App) {
    let area = centered_rect(50, 30, frame.area());
    frame.render_widget(Clear, area);

    let workflow_name = app
        .selected_workflow_id
        .as_ref()
        .and_then(|id| app.workflows.iter().find(|w| &w.id == id))
        .map(|w| w.name.as_str())
        .unwrap_or("Unknown");

    let text = format!(
        "\n\n  Execute workflow:\n\n  \"{}\"\n\n  [Enter] Confirm  │  [Esc] Cancel",
        workflow_name
    );

    let dialog = Paragraph::new(text)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Cyan))
                .title(Span::styled(
                    " Execute Workflow ",
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                )),
        )
        .style(Style::default().fg(Color::White))
        .alignment(Alignment::Center);

    frame.render_widget(dialog, area);
}

fn render_delete_dialog(frame: &mut Frame, app: &App) {
    let area = centered_rect(50, 30, frame.area());
    frame.render_widget(Clear, area);

    let workflow_name = app
        .selected_workflow_id
        .as_ref()
        .and_then(|id| app.workflows.iter().find(|w| &w.id == id))
        .map(|w| w.name.as_str())
        .unwrap_or("Unknown");

    let text = format!("\n\n  Delete workflow:\n\n  \"{}\"?\n\n  This action cannot be undone.\n\n  [Enter] Confirm  │  [Esc] Cancel", workflow_name);

    let dialog = Paragraph::new(text)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Red))
                .title(Span::styled(
                    " Delete Workflow ",
                    Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
                )),
        )
        .style(Style::default().fg(Color::White))
        .alignment(Alignment::Center);

    frame.render_widget(dialog, area);
}

fn render_cancel_dialog(frame: &mut Frame, app: &App) {
    let area = centered_rect(50, 30, frame.area());
    frame.render_widget(Clear, area);

    let exec_name = app
        .selected_execution_id
        .as_ref()
        .and_then(|id| app.executions.iter().find(|e| &e.id == id))
        .map(|e| e.workflow_name.as_str())
        .unwrap_or("Unknown");

    let text = format!(
        "\n\n  Cancel execution:\n\n  \"{}\"?\n\n  [Enter] Confirm  │  [Esc] Cancel",
        exec_name
    );

    let dialog = Paragraph::new(text)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Yellow))
                .title(Span::styled(
                    " Cancel Execution ",
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                )),
        )
        .style(Style::default().fg(Color::White))
        .alignment(Alignment::Center);

    frame.render_widget(dialog, area);
}

fn render_error_dialog(frame: &mut Frame, app: &App) {
    let area = centered_rect(60, 30, frame.area());
    frame.render_widget(Clear, area);

    let text = format!(
        "\n\n  {}\n\n  [Esc] Close",
        app.error_message.as_deref().unwrap_or("An error occurred")
    );

    let dialog = Paragraph::new(text)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Red))
                .title(Span::styled(
                    " Error ",
                    Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
                )),
        )
        .style(Style::default().fg(Color::White))
        .alignment(Alignment::Center);

    frame.render_widget(dialog, area);
}

fn create_progress_bar(progress: f64, width: usize) -> String {
    let filled = (progress * width as f64).round() as usize;
    let empty = width.saturating_sub(filled);
    format!("[{}{}]", "█".repeat(filled), "░".repeat(empty))
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() > max {
        format!("{}…", &s[..max - 1])
    } else {
        s.to_string()
    }
}

fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(r);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}

// ============================================================================
// Main Entry Point
// ============================================================================

#[tokio::main]
async fn main() -> Result<()> {
    // Parse arguments
    let args = Args::parse();

    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Create app
    let mut app = App::new(
        args.api_url,
        Duration::from_millis(args.refresh),
        args.debug,
    );

    // Initial data fetch
    app.refresh_data().await;

    // Main loop
    let tick_rate = Duration::from_millis(100);
    let mut last_tick = Instant::now();

    loop {
        terminal.draw(|f| ui(f, &mut app))?;

        let timeout = tick_rate
            .checked_sub(last_tick.elapsed())
            .unwrap_or_else(|| Duration::from_secs(0));

        if crossterm::event::poll(timeout)? {
            if let Event::Key(key) = event::read()? {
                // Handle dialog first
                if app.dialog_type != DialogType::None {
                    match key.code {
                        KeyCode::Enter => match app.dialog_type {
                            DialogType::Execute => app.execute_selected_workflow().await,
                            DialogType::Delete => app.delete_selected_workflow().await,
                            DialogType::Cancel => app.cancel_selected_execution().await,
                            _ => app.dialog_type = DialogType::None,
                        },
                        KeyCode::Esc => {
                            app.dialog_type = DialogType::None;
                            app.selected_workflow_id = None;
                            app.selected_execution_id = None;
                        }
                        _ => {}
                    }
                    continue;
                }

                match key.code {
                    KeyCode::Char('q') | KeyCode::Char('Q') => app.should_quit = true,
                    KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                        app.should_quit = true
                    }
                    KeyCode::Tab => {
                        if key.modifiers.contains(KeyModifiers::SHIFT) {
                            app.prev_tab();
                        } else {
                            app.next_tab();
                        }
                    }
                    KeyCode::Char('1') => app.current_tab = Tab::Dashboard,
                    KeyCode::Char('2') => app.current_tab = Tab::Workflows,
                    KeyCode::Char('3') => app.current_tab = Tab::Executions,
                    KeyCode::Char('4') => app.current_tab = Tab::Logs,
                    KeyCode::Char('5') | KeyCode::Char('?') => app.current_tab = Tab::Help,
                    KeyCode::Down | KeyCode::Char('j') => app.next_item(),
                    KeyCode::Up | KeyCode::Char('k') => app.prev_item(),
                    KeyCode::Enter | KeyCode::Char('x') => app.select_item(),
                    KeyCode::Char('d') => app.show_delete_dialog(),
                    KeyCode::Char('c') => app.show_cancel_dialog(),
                    KeyCode::Char('r') => app.refresh_data().await,
                    KeyCode::Esc => {
                        if app.current_tab == Tab::Help {
                            app.current_tab = Tab::Dashboard;
                        }
                    }
                    _ => {}
                }
            }
        }

        if last_tick.elapsed() >= tick_rate {
            last_tick = Instant::now();
        }

        // Auto-refresh
        if app.last_refresh.elapsed() >= app.refresh_interval {
            app.refresh_data().await;
        }

        if app.should_quit {
            break;
        }
    }

    // Restore terminal
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    Ok(())
}
