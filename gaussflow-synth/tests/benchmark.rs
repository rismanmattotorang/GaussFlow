//! Phase S hardening: a synthesis benchmark suite / regression gate.
//!
//! Each case pairs a prompt with the plan(s) a model would return (canned, since planning quality
//! needs a live LLM) and an input. We push every case through the *real* pipeline — synthesize
//! (with repair) → validate → run on the runtime — and report first-pass validation rate, average
//! repair iterations, and run-success rate, asserting regression thresholds.

use async_trait::async_trait;
use gaussflow_runtime::provider::{CompletionRequest, LlmProvider, ProviderError};
use gaussflow_synth::{run, SynthesisRequest, Synthesizer};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};

struct ScriptedProvider {
    responses: Vec<String>,
    calls: AtomicUsize,
}
impl ScriptedProvider {
    fn new(responses: &[&str]) -> Self {
        Self {
            responses: responses.iter().map(|s| s.to_string()).collect(),
            calls: AtomicUsize::new(0),
        }
    }
}
#[async_trait]
impl LlmProvider for ScriptedProvider {
    async fn complete(&self, _req: &CompletionRequest) -> Result<String, ProviderError> {
        let i = self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.responses.get(i).cloned().unwrap_or_default())
    }
    fn name(&self) -> &'static str {
        "scripted"
    }
}

struct BenchCase {
    name: &'static str,
    prompt: &'static str,
    /// Canned model outputs, in order (more than one exercises the repair loop).
    responses: Vec<&'static str>,
    input: Value,
}

fn corpus() -> Vec<BenchCase> {
    vec![
        BenchCase {
            name: "summarize",
            prompt: "extract text then summarize",
            responses: vec![
                r#"{"name":"s","steps":[
                    {"id":"ingest","capability":"data_processor","depends_on":[],"params":{"op":"extract","field":"text"}},
                    {"id":"sum","capability":"llm_call","depends_on":["ingest"],"params":{"model":"mock-s","prompt":"summarize"}}]}"#,
            ],
            input: json!({ "text": "hello" }),
        },
        BenchCase {
            name: "branch",
            prompt: "approve if score > 3 else reject",
            responses: vec![
                r#"{"name":"b","steps":[
                    {"id":"check","capability":"conditional","depends_on":[],"params":{"field":"score","op":"gt","value":3},"edges":{"approve":"true","reject":"false"}},
                    {"id":"approve","capability":"data_processor","depends_on":["check"],"params":{"op":"set","value":{"ok":true}}},
                    {"id":"reject","capability":"data_processor","depends_on":["check"],"params":{"op":"set","value":{"ok":false}}}]}"#,
            ],
            input: json!({ "score": 5 }),
        },
        BenchCase {
            name: "route",
            prompt: "route gold to premium else standard",
            responses: vec![
                r#"{"name":"r","steps":[
                    {"id":"router","capability":"router","depends_on":[],"params":{"field":"tier","routes":{"gold":"premium"},"default":"standard"},"edges":{"premium":"premium","standard":"standard"}},
                    {"id":"premium","capability":"data_processor","depends_on":["router"],"params":{}},
                    {"id":"standard","capability":"data_processor","depends_on":["router"],"params":{}}]}"#,
            ],
            input: json!({ "tier": "gold" }),
        },
        BenchCase {
            name: "ensemble",
            prompt: "ask three models and aggregate",
            responses: vec![
                r#"{"name":"e","steps":[
                    {"id":"m1","capability":"data_processor","depends_on":[],"params":{"op":"set","value":{"label":"a"}}},
                    {"id":"m2","capability":"data_processor","depends_on":[],"params":{"op":"set","value":{"label":"a"}}},
                    {"id":"agg","capability":"ensemble","depends_on":["m1","m2"],"params":{"strategy":"collect"}}]}"#,
            ],
            input: json!({}),
        },
        BenchCase {
            name: "agent",
            prompt: "shout the greeting using a tool",
            responses: vec![
                r#"{"name":"a","steps":[
                    {"id":"act","capability":"agent","depends_on":[],"params":{"task":"shout","script":[{"tool":"upper","args":{"text":"hi"}},{"final":"done"}]}}]}"#,
            ],
            input: json!({}),
        },
        BenchCase {
            name: "parallel",
            prompt: "do two things at once",
            responses: vec![
                r#"{"name":"p","steps":[
                    {"id":"fan","capability":"parallel","depends_on":[],"params":{"branches":[
                        {"name":"x","nodes":[{"id":"a","type":"data_processor","params":{"op":"set","value":{"b":1}}}],"connections":[],"settings":{}},
                        {"name":"y","nodes":[{"id":"c","type":"data_processor","params":{"op":"set","value":{"b":2}}}],"connections":[],"settings":{}}]}}]}"#,
            ],
            input: json!({}),
        },
        BenchCase {
            name: "repair",
            prompt: "summarize (first plan is invalid, then repaired)",
            responses: vec![
                // First attempt uses an unsupported capability → must be repaired.
                r#"{"name":"bad","steps":[{"id":"x","capability":"telepathy","depends_on":[],"params":{}}]}"#,
                r#"{"name":"ok","steps":[{"id":"sum","capability":"llm_call","depends_on":[],"params":{"model":"mock-r","prompt":"summarize"}}]}"#,
            ],
            input: json!({}),
        },
    ]
}

#[tokio::test]
async fn synthesis_benchmark_meets_thresholds() {
    let cases = corpus();
    let total = cases.len();
    let mut validated = 0usize;
    let mut first_pass = 0usize;
    let mut run_succeeded = 0usize;
    let mut total_repairs = 0usize;

    println!("\n=== GaussFlow synthesis benchmark ===");
    for case in &cases {
        let provider = ScriptedProvider::new(&case.responses);
        let synth = Synthesizer::new(&provider).with_max_repairs(2);
        let req = SynthesisRequest::new(case.prompt);

        let result = synth.synthesize(&req).await;
        let calls = provider.calls.load(Ordering::SeqCst);
        let repairs = calls.saturating_sub(1);
        total_repairs += repairs;

        let (ok_validate, ok_run) = match result {
            Ok(r) => {
                validated += 1;
                if repairs == 0 {
                    first_pass += 1;
                }
                let ran = run(&r, case.input.clone()).await.is_ok();
                if ran {
                    run_succeeded += 1;
                }
                (true, ran)
            }
            Err(_) => (false, false),
        };
        println!(
            "  {:<10} validate={:<5} run={:<5} repairs={}",
            case.name, ok_validate, ok_run, repairs
        );
    }

    let pct = |n: usize| 100.0 * n as f64 / total as f64;
    println!("--- summary ---");
    println!("  cases:              {total}");
    println!("  validated:          {validated} ({:.0}%)", pct(validated));
    println!(
        "  first-pass (no repair): {first_pass} ({:.0}%)",
        pct(first_pass)
    );
    println!(
        "  run succeeded:      {run_succeeded} ({:.0}%)",
        pct(run_succeeded)
    );
    println!(
        "  avg repairs/case:   {:.2}",
        total_repairs as f64 / total as f64
    );

    // Regression gates for this curated corpus.
    assert_eq!(
        validated, total,
        "every case should synthesize to a valid workflow"
    );
    assert_eq!(
        run_succeeded, total,
        "every synthesized workflow should run"
    );
    assert_eq!(
        first_pass,
        total - 1,
        "all but the dedicated repair case should validate on the first attempt"
    );
}
