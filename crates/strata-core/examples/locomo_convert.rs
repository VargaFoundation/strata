//! Convert a real **LoCoMo** or **LongMemEval** export into the `locomo_eval` harness schema.
//!
//!   cargo run -p strata-core --example locomo_convert -- <input.json> [locomo|longmemeval] > out.json
//!   LOCOMO_PATH=out.json cargo run -p strata-core --example locomo_eval
//!
//! The second arg is optional — the format is auto-detected from the JSON shape otherwise.
//!
//! Output schema (consumed by `locomo_eval`): an array of conversations
//!   { "user": "...", "turns": ["speaker: text", …], "qa": [ {"question","expected","category"} ] }
//!
//! Public dataset shapes this handles (navigated defensively via `serde_json::Value`, since field
//! names vary slightly across releases — eyeball the output and adjust if your export differs):
//!   - LoCoMo (snap-research/locomo): items with `conversation` = { session_1: [{speaker,text}], … }
//!     and `qa` = [ {question, answer|adversarial_answer, category, …} ].
//!   - LongMemEval (xiaowu0162/LongMemEval): items with `haystack_sessions` = [ [ {role,content} ] ]
//!     and `question`, `answer`, `question_type` (one QA per item).

use serde_json::{json, Value};

fn main() {
    let mut args = std::env::args().skip(1);
    let path = match args.next() {
        Some(p) => p,
        None => {
            eprintln!("usage: locomo_convert <input.json> [locomo|longmemeval] > out.json");
            std::process::exit(2);
        }
    };
    let forced = args.next();
    let raw = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        eprintln!("read {path}: {e}");
        std::process::exit(1);
    });
    let input: Value = serde_json::from_str(&raw).unwrap_or_else(|e| {
        eprintln!("parse {path}: {e}");
        std::process::exit(1);
    });
    let items = input.as_array().cloned().unwrap_or_else(|| vec![input]);
    let fmt = forced.unwrap_or_else(|| detect(&items));
    eprintln!("format: {fmt}  ({} items)", items.len());

    let convos: Vec<Value> = items
        .iter()
        .enumerate()
        .map(|(i, it)| match fmt.as_str() {
            "longmemeval" => convert_longmemeval(i, it),
            _ => convert_locomo(i, it),
        })
        .collect();

    let qa_total: usize = convos
        .iter()
        .filter_map(|c| c.get("qa").and_then(|v| v.as_array()).map(|a| a.len()))
        .sum();
    eprintln!("→ {} conversations, {qa_total} questions", convos.len());
    println!("{}", serde_json::to_string_pretty(&convos).expect("serialize"));
}

/// Auto-detect the source format from the presence of LongMemEval-specific keys.
fn detect(items: &[Value]) -> String {
    let is_lme = items
        .iter()
        .any(|it| it.get("haystack_sessions").is_some() || it.get("question_type").is_some());
    if is_lme { "longmemeval" } else { "locomo" }.to_string()
}

/// Stringify a scalar JSON value (string as-is, number/bool to text); arrays join their elements.
fn value_to_string(v: Option<&Value>) -> Option<String> {
    match v? {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Array(a) => Some(
            a.iter()
                .filter_map(|e| value_to_string(Some(e)))
                .collect::<Vec<_>>()
                .join(", "),
        ),
        _ => None,
    }
}

/// A plain category label (string, or stringified number for LoCoMo's numeric categories).
fn category_of(v: Option<&Value>) -> Option<String> {
    value_to_string(v)
}

/// Render one conversation turn as "speaker: text".
fn turn_line(turn: &Value) -> Option<String> {
    let speaker = turn
        .get("speaker")
        .or_else(|| turn.get("role"))
        .and_then(|v| v.as_str())
        .unwrap_or("speaker");
    let text = turn
        .get("text")
        .or_else(|| turn.get("clean_text"))
        .or_else(|| turn.get("content"))
        .and_then(|v| v.as_str())?;
    Some(format!("{speaker}: {text}"))
}

/// Numeric suffix of a `session_N` key, for chronological ordering (non-numeric → large).
fn session_num(key: &str) -> u64 {
    key.rsplit(['_', '-'])
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or(u64::MAX)
}

fn convert_locomo(i: usize, item: &Value) -> Value {
    let user = item
        .get("sample_id")
        .and_then(|v| v.as_str())
        .map(String::from)
        .unwrap_or_else(|| format!("locomo_{i}"));

    let mut turns = Vec::new();
    if let Some(conv) = item.get("conversation").and_then(|v| v.as_object()) {
        let mut keys: Vec<&String> = conv.keys().filter(|k| k.starts_with("session")).collect();
        keys.sort_by_key(|k| session_num(k));
        for k in keys {
            if let Some(arr) = conv.get(k).and_then(|v| v.as_array()) {
                turns.extend(arr.iter().filter_map(turn_line));
            }
        }
    }

    let qa = item
        .get("qa")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|q| {
                    let question = q.get("question")?.as_str()?.to_string();
                    let expected =
                        value_to_string(q.get("answer").or_else(|| q.get("adversarial_answer")))?;
                    Some(json!({
                        "question": question,
                        "expected": expected,
                        "category": category_of(q.get("category")),
                    }))
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    json!({ "user": user, "turns": turns, "qa": qa })
}

fn convert_longmemeval(i: usize, item: &Value) -> Value {
    let user = item
        .get("question_id")
        .and_then(|v| v.as_str())
        .map(String::from)
        .unwrap_or_else(|| format!("lme_{i}"));

    let mut turns = Vec::new();
    if let Some(sessions) = item.get("haystack_sessions").and_then(|v| v.as_array()) {
        for session in sessions {
            if let Some(arr) = session.as_array() {
                turns.extend(arr.iter().filter_map(turn_line));
            }
        }
    }

    let question = item
        .get("question")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let expected = value_to_string(item.get("answer")).unwrap_or_default();
    json!({
        "user": user,
        "turns": turns,
        "qa": [ { "question": question, "expected": expected, "category": category_of(item.get("question_type")) } ],
    })
}
