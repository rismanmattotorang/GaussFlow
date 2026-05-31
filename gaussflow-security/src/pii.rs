//! Lightweight PII redaction for data flowing into logs/storage.
//!
//! [`redact_json`] walks a JSON document and replaces email-like substrings inside any string with
//! `[REDACTED_EMAIL]`, preserving surrounding text and structure. Regex-free and deterministic.
//! (A production system would extend this with more detectors and a policy; this is the wired-in
//! baseline the data path can call.)

use serde_json::{Map, Value};

/// Recursively redact likely-PII from all strings in a JSON value.
pub fn redact_json(value: &Value) -> Value {
    match value {
        Value::String(s) => Value::String(redact_str(s)),
        Value::Array(items) => Value::Array(items.iter().map(redact_json).collect()),
        Value::Object(obj) => {
            let mut out = Map::with_capacity(obj.len());
            for (k, v) in obj {
                out.insert(k.clone(), redact_json(v));
            }
            Value::Object(out)
        }
        other => other.clone(),
    }
}

/// Redact email-like tokens within a single string, preserving everything else.
pub fn redact_str(s: &str) -> String {
    let chars: Vec<char> = s.chars().collect();
    let is_email_char =
        |c: char| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '%' | '+' | '-' | '@');

    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < chars.len() {
        if is_email_char(chars[i]) {
            let start = i;
            while i < chars.len() && is_email_char(chars[i]) {
                i += 1;
            }
            let token: String = chars[start..i].iter().collect();
            if looks_like_email(&token) {
                out.push_str("[REDACTED_EMAIL]");
            } else {
                out.push_str(&token);
            }
        } else {
            out.push(chars[i]);
            i += 1;
        }
    }
    out
}

fn looks_like_email(token: &str) -> bool {
    let mut parts = token.split('@');
    match (parts.next(), parts.next(), parts.next()) {
        (Some(local), Some(domain), None) => {
            !local.is_empty()
                && domain.contains('.')
                && !domain.starts_with('.')
                && !domain.ends_with('.')
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn redacts_emails_preserving_structure() {
        assert_eq!(
            redact_str("contact a.b@example.com now"),
            "contact [REDACTED_EMAIL] now"
        );
        assert_eq!(redact_str("(ada@corp.io)"), "([REDACTED_EMAIL])");
        // Not emails.
        assert_eq!(redact_str("no pii here"), "no pii here");
        assert_eq!(redact_str("user@localhost"), "user@localhost"); // no dot in domain
    }

    #[test]
    fn redacts_recursively_in_json() {
        let input = json!({
            "user": "me@x.com",
            "nested": { "list": ["clean", "you@y.org"] },
            "count": 3
        });
        let out = redact_json(&input);
        assert_eq!(out["user"], json!("[REDACTED_EMAIL]"));
        assert_eq!(out["nested"]["list"][1], json!("[REDACTED_EMAIL]"));
        assert_eq!(out["nested"]["list"][0], json!("clean"));
        assert_eq!(out["count"], json!(3)); // non-strings untouched
    }
}
