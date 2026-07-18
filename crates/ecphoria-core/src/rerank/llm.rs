//! LLM-backed reranker — reuses a [`CompletionProvider`] to judge passage relevance.
//!
//! Zero new dependencies: it drives the same Ollama/OpenAI completion backends the cognition
//! layer already uses. The model scores each numbered passage 0–10 for relevance and returns a
//! JSON array; scores are parsed leniently (missing/garbled entries → 0). If the whole reply
//! can't be parsed the call returns `Err`, so the caller falls back to the unreranked order.

use std::fmt::Write as _;
use std::sync::Arc;

use serde::Deserialize;

use super::Reranker;
use crate::llm::CompletionProvider;

/// Max characters of each passage sent to the model (keeps the prompt bounded).
const MAX_DOC_CHARS: usize = 480;

const SYSTEM: &str = "You are a search relevance judge. Given a query and a numbered list of \
passages, rate how well each passage answers the query from 0 (irrelevant) to 10 (directly \
answers it). Respond with ONLY a JSON array of objects like \
[{\"i\":0,\"score\":7},{\"i\":1,\"score\":0}] — exactly one entry per passage, no prose.";

/// Reranker that asks a chat-completion model to score relevance.
pub struct LlmReranker {
    completion: Arc<dyn CompletionProvider>,
}

impl LlmReranker {
    pub fn new(completion: Arc<dyn CompletionProvider>) -> Self {
        Self { completion }
    }
}

#[derive(Deserialize)]
struct ScoreItem {
    i: usize,
    score: f32,
}

/// Extract the first top-level JSON array from a model reply and parse it into per-doc scores.
/// Returns `None` if no array is found or it doesn't parse; missing indices default to 0.0.
fn parse_scores(reply: &str, n: usize) -> Option<Vec<f32>> {
    let start = reply.find('[')?;
    let end = reply.rfind(']')?;
    if end <= start {
        return None;
    }
    let items: Vec<ScoreItem> = serde_json::from_str(&reply[start..=end]).ok()?;
    let mut scores = vec![0.0_f32; n];
    for it in items {
        if it.i < n {
            scores[it.i] = it.score;
        }
    }
    Some(scores)
}

#[async_trait::async_trait]
impl Reranker for LlmReranker {
    async fn rerank(&self, query: &str, docs: &[String]) -> crate::Result<Vec<f32>> {
        if docs.is_empty() {
            return Ok(Vec::new());
        }
        let mut user = String::new();
        let _ = writeln!(user, "Query: {query}\n\nPassages:");
        for (i, d) in docs.iter().enumerate() {
            let snippet: String = d.chars().take(MAX_DOC_CHARS).collect();
            let _ = writeln!(user, "[{i}] {snippet}");
        }
        let reply = self.completion.complete(SYSTEM, &user).await?;
        parse_scores(&reply, docs.len()).ok_or_else(|| {
            crate::Error::Llm(format!(
                "reranker: could not parse scores from reply: {reply:?}"
            ))
        })
    }

    fn model_name(&self) -> &str {
        self.completion.model_name()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Canned(String);

    #[async_trait::async_trait]
    impl CompletionProvider for Canned {
        async fn complete(&self, _system: &str, _user: &str) -> crate::Result<String> {
            Ok(self.0.clone())
        }
        fn model_name(&self) -> &str {
            "canned"
        }
    }

    fn reranker(reply: &str) -> LlmReranker {
        LlmReranker::new(Arc::new(Canned(reply.into())))
    }

    #[tokio::test]
    async fn parses_scores_in_order() {
        let rr = reranker(r#"[{"i":0,"score":2},{"i":1,"score":9},{"i":2,"score":5}]"#);
        let docs = vec!["a".into(), "b".into(), "c".into()];
        assert_eq!(rr.rerank("q", &docs).await.unwrap(), vec![2.0, 9.0, 5.0]);
    }

    #[test]
    fn lenient_parse_fills_missing_with_zero() {
        // Surrounding prose + a missing index → still parses, gaps default to 0.0.
        let scores = parse_scores("here you go: [{\"i\":1,\"score\":7}] done", 3).unwrap();
        assert_eq!(scores, vec![0.0, 7.0, 0.0]);
    }

    #[test]
    fn out_of_range_index_ignored() {
        let scores = parse_scores(r#"[{"i":0,"score":4},{"i":9,"score":8}]"#, 2).unwrap();
        assert_eq!(scores, vec![4.0, 0.0]);
    }

    #[tokio::test]
    async fn unparseable_reply_errors() {
        let rr = reranker("no json here");
        assert!(rr.rerank("q", &["a".to_string()]).await.is_err());
    }

    #[tokio::test]
    async fn empty_docs_short_circuit() {
        let rr = reranker("");
        assert_eq!(rr.rerank("q", &[]).await.unwrap(), Vec::<f32>::new());
    }
}
