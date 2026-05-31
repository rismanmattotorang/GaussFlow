//! Template module for GaussFlow WebUI
//! 
//! This module provides template utilities for server-side rendering.
//! Currently, the main UI is served as a static HTML file with client-side
//! JavaScript, but this module can be expanded for SSR needs.

use serde::Serialize;

/// Template context for error pages
#[derive(Debug, Clone, Serialize)]
pub struct ErrorContext {
    pub title: String,
    pub message: String,
    pub code: u16,
}

impl ErrorContext {
    pub fn new(code: u16, message: impl Into<String>) -> Self {
        let title = match code {
            404 => "Not Found",
            500 => "Internal Server Error",
            403 => "Forbidden",
            401 => "Unauthorized",
            _ => "Error",
        };
        
        Self {
            title: title.to_string(),
            message: message.into(),
            code,
        }
    }
}

/// Generate a simple error page HTML
pub fn error_page(ctx: &ErrorContext) -> String {
    format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{} - GaussFlow</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #0a0a0f;
            color: #fff;
            display: flex;
            align-items: center;
            justify-content: center;
            min-height: 100vh;
            margin: 0;
        }}
        .error-container {{
            text-align: center;
            padding: 40px;
        }}
        .error-code {{
            font-size: 120px;
            font-weight: 700;
            background: linear-gradient(135deg, #00f5d4, #7b2cbf);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            margin: 0;
        }}
        .error-title {{
            font-size: 24px;
            margin: 20px 0 10px;
        }}
        .error-message {{
            color: #a0a0b0;
            margin-bottom: 30px;
        }}
        .back-link {{
            display: inline-block;
            padding: 12px 24px;
            background: linear-gradient(135deg, #00f5d4, #00d4b8);
            color: #0a0a0f;
            text-decoration: none;
            border-radius: 8px;
            font-weight: 500;
        }}
        .back-link:hover {{
            transform: translateY(-2px);
            box-shadow: 0 0 30px rgba(0, 245, 212, 0.4);
        }}
    </style>
</head>
<body>
    <div class="error-container">
        <h1 class="error-code">{}</h1>
        <h2 class="error-title">{}</h2>
        <p class="error-message">{}</p>
        <a href="/" class="back-link">Back to Dashboard</a>
    </div>
</body>
</html>"#,
        ctx.title, ctx.code, ctx.title, ctx.message
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_context() {
        let ctx = ErrorContext::new(404, "Page not found");
        assert_eq!(ctx.code, 404);
        assert_eq!(ctx.title, "Not Found");
    }

    #[test]
    fn test_error_page_generation() {
        let ctx = ErrorContext::new(500, "Something went wrong");
        let html = error_page(&ctx);
        assert!(html.contains("500"));
        assert!(html.contains("Internal Server Error"));
    }
}
