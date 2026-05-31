use anyhow::{Context, Result};
use console::style;
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;
use tracing::{debug, error, info, warn};

/// Utility functions for the CLI

/// Format JSON output with colors
pub fn format_json(value: &Value) -> Result<String> {
    let json_str = serde_json::to_string_pretty(value)?;
    
    // Add basic syntax highlighting
    let highlighted = json_str
        .lines()
        .map(|line| {
            if line.contains("\"") {
                // Highlight strings
                line.replace("\"", &style("\"").cyan().to_string())
            } else if line.contains(":") {
                // Highlight keys
                let parts: Vec<&str> = line.splitn(2, ":").collect();
                if parts.len() == 2 {
                    format!("{}{}{}", 
                        style(parts[0]).yellow(),
                        style(":").white(),
                        parts[1]
                    )
                } else {
                    line.to_string()
                }
            } else {
                line.to_string()
            }
        })
        .collect::<Vec<String>>()
        .join("\n");
    
    Ok(highlighted)
}

/// Format table output
pub fn format_table(headers: Vec<String>, rows: Vec<Vec<String>>) -> String {
    if rows.is_empty() {
        return "No data available".to_string();
    }
    
    // Calculate column widths
    let mut column_widths = vec![0; headers.len()];
    
    // Check header widths
    for (i, header) in headers.iter().enumerate() {
        column_widths[i] = column_widths[i].max(header.len());
    }
    
    // Check row widths
    for row in &rows {
        for (i, cell) in row.iter().enumerate() {
            if i < column_widths.len() {
                column_widths[i] = column_widths[i].max(cell.len());
            }
        }
    }
    
    // Build table
    let mut table = String::new();
    
    // Header
    table.push_str(&format_table_row(&headers, &column_widths));
    table.push('\n');
    
    // Separator
    table.push_str(&format_table_separator(&column_widths));
    table.push('\n');
    
    // Rows
    for row in rows {
        table.push_str(&format_table_row(&row, &column_widths));
        table.push('\n');
    }
    
    table
}

/// Format a table row
fn format_table_row(cells: &[String], widths: &[usize]) -> String {
    let mut row = String::new();
    row.push('|');
    
    for (i, cell) in cells.iter().enumerate() {
        let width = if i < widths.len() { widths[i] } else { cell.len() };
        row.push_str(&format!(" {:<width$} |", cell, width = width));
    }
    
    row
}

/// Format table separator
fn format_table_separator(widths: &[usize]) -> String {
    let mut separator = String::new();
    separator.push('|');
    
    for &width in widths {
        separator.push_str(&format!("{:-<width$}|", "", width = width + 2));
    }
    
    separator
}

/// Validate file path
pub fn validate_file_path(path: &PathBuf) -> Result<()> {
    if !path.exists() {
        anyhow::bail!("File does not exist: {}", path.display());
    }
    
    if !path.is_file() {
        anyhow::bail!("Path is not a file: {}", path.display());
    }
    
    Ok(())
}

/// Validate directory path
pub fn validate_directory_path(path: &PathBuf) -> Result<()> {
    if !path.exists() {
        anyhow::bail!("Directory does not exist: {}", path.display());
    }
    
    if !path.is_dir() {
        anyhow::bail!("Path is not a directory: {}", path.display());
    }
    
    Ok(())
}

/// Create directory if it doesn't exist
pub async fn ensure_directory(path: &PathBuf) -> Result<()> {
    if !path.exists() {
        tokio::fs::create_dir_all(path).await
            .with_context(|| format!("Failed to create directory: {}", path.display()))?;
        info!("Created directory: {}", path.display());
    }
    
    Ok(())
}

/// Parse JSON input
pub fn parse_json_input(input: &str) -> Result<Value> {
    if input.trim().is_empty() {
        Ok(Value::Null)
    } else {
        serde_json::from_str(input)
            .with_context(|| format!("Failed to parse JSON input: {}", input))
    }
}

/// Format duration for display
pub fn format_duration(duration: std::time::Duration) -> String {
    if duration.as_secs() < 1 {
        format!("{:.2}ms", duration.as_millis())
    } else if duration.as_secs() < 60 {
        format!("{:.2}s", duration.as_secs_f64())
    } else if duration.as_secs() < 3600 {
        let minutes = duration.as_secs() / 60;
        let seconds = duration.as_secs() % 60;
        format!("{}m {}s", minutes, seconds)
    } else {
        let hours = duration.as_secs() / 3600;
        let minutes = (duration.as_secs() % 3600) / 60;
        format!("{}h {}m", hours, minutes)
    }
}

/// Format bytes for display
pub fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    
    if bytes < KB {
        format!("{} B", bytes)
    } else if bytes < MB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else if bytes < GB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    }
}

/// Confirm user action
pub async fn confirm_action(prompt: &str) -> Result<bool> {
    use std::io::{self, Write};
    
    print!("{} (y/N): ", prompt);
    io::stdout().flush()?;
    
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    
    let input = input.trim().to_lowercase();
    Ok(input == "y" || input == "yes")
}

/// Show progress with spinner
pub fn show_progress(message: &str) -> indicatif::ProgressBar {
    let pb = indicatif::ProgressBar::new_spinner();
    pb.set_style(
        indicatif::ProgressStyle::default_spinner()
            .template("{spinner:.green} {wide_msg}")
            .unwrap()
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"),
    );
    pb.set_message(message.to_string());
    pb
}

/// Show error with styling
pub fn show_error(message: &str) {
    eprintln!("{}", format!("{}", style("Error: ").red().bold()) + message);
}

/// Show success with styling
pub fn show_success(message: &str) {
    println!("{}", format!("{}", style("✓ ").green().bold()) + message);
}

/// Show warning with styling
pub fn show_warning(message: &str) {
    println!("{}", format!("{}", style("⚠ ").yellow().bold()) + message);
}

/// Show info with styling
pub fn show_info(message: &str) {
    println!("{}", format!("{}", style("ℹ ").blue().bold()) + message);
}

/// Parse key-value pairs from string
pub fn parse_key_value_pairs(input: &str) -> Result<HashMap<String, String>> {
    let mut pairs = HashMap::new();
    
    for line in input.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        
        if let Some((key, value)) = line.split_once('=') {
            let key = key.trim().to_string();
            let value = value.trim().to_string();
            pairs.insert(key, value);
        } else {
            warn!("Invalid key-value pair: {}", line);
        }
    }
    
    Ok(pairs)
}

/// Generate a unique ID
pub fn generate_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    
    let random = rand::random::<u32>();
    
    format!("{}_{:x}", timestamp, random)
}

/// Check if running in a terminal
pub fn is_terminal() -> bool {
    atty::is(atty::Stream::Stdout)
}

/// Get terminal size
pub fn get_terminal_size() -> Option<(usize, usize)> {
    term_size::dimensions()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    
    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(std::time::Duration::from_millis(500)), "500.00ms");
        assert_eq!(format_duration(std::time::Duration::from_secs(30)), "30.00s");
        assert_eq!(format_duration(std::time::Duration::from_secs(90)), "1m 30s");
        assert_eq!(format_duration(std::time::Duration::from_secs(3661)), "1h 1m");
    }
    
    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1024 * 1024), "1.00 MB");
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.00 GB");
    }
    
    #[test]
    fn test_parse_key_value_pairs() {
        let input = "key1=value1\nkey2=value2\n# comment\n";
        let pairs = parse_key_value_pairs(input).unwrap();
        
        assert_eq!(pairs.get("key1"), Some(&"value1".to_string()));
        assert_eq!(pairs.get("key2"), Some(&"value2".to_string()));
        assert_eq!(pairs.len(), 2);
    }
    
    #[test]
    fn test_format_table() {
        let headers = vec!["Name".to_string(), "Age".to_string()];
        let rows = vec![
            vec!["Alice".to_string(), "25".to_string()],
            vec!["Bob".to_string(), "30".to_string()],
        ];
        
        let table = format_table(headers, rows);
        assert!(table.contains("Alice"));
        assert!(table.contains("Bob"));
        assert!(table.contains("|"));
    }
} 