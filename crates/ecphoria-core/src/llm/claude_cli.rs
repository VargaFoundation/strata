//! Claude Code CLI completion provider — shells out to the authenticated `claude -p` (non-interactive
//! "print" mode). Uses the local CLI's existing auth (subscription/OAuth), so **no API key is
//! needed**. Slower per call than the HTTP API (process startup), but handy for evals/dev on a
//! machine where `claude` is logged in.

use std::process::Stdio;

use tokio::io::AsyncWriteExt;
use tokio::process::Command;

/// Completion provider that invokes `claude -p --append-system-prompt <system> --model <model>` with
/// the user message piped on stdin.
pub struct ClaudeCliCompletion {
    bin: String,
    model: String,
}

impl ClaudeCliCompletion {
    pub fn new(model: String) -> Self {
        Self {
            bin: std::env::var("ECPHORIA_CLAUDE_BIN").unwrap_or_else(|_| "claude".into()),
            model,
        }
    }
}

#[async_trait::async_trait]
impl super::CompletionProvider for ClaudeCliCompletion {
    async fn complete(&self, system: &str, user: &str) -> crate::Result<String> {
        let mut child = Command::new(&self.bin)
            .arg("-p")
            .arg("--append-system-prompt")
            .arg(system)
            .arg("--model")
            .arg(&self.model)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| crate::Error::Llm(format!("claude CLI spawn failed: {e}")))?;

        // Feed the prompt on stdin (avoids ARG_MAX for large RAG contexts), then close it (EOF).
        // The Claude Code CLI already ships a large *agent* system prompt; `--append-system-prompt`
        // only appends to it, so a strict instruction there (e.g. "output ONLY a JSON array") is
        // reliably overridden by the agent persona — the CLI answers conversationally instead, and
        // structured callers (extraction/rerank/judge) silently get unparseable prose. Inlining the
        // system prompt into the user turn is honoured, so we prepend it here (belt-and-suspenders
        // with the flag above).
        {
            let mut stdin = child
                .stdin
                .take()
                .ok_or_else(|| crate::Error::Llm("claude CLI: no stdin handle".into()))?;
            let payload = compose_prompt(system, user);
            stdin
                .write_all(payload.as_bytes())
                .await
                .map_err(|e| crate::Error::Llm(format!("claude CLI stdin write: {e}")))?;
        }

        let out = child
            .wait_with_output()
            .await
            .map_err(|e| crate::Error::Llm(format!("claude CLI wait: {e}")))?;
        if !out.status.success() {
            return Err(crate::Error::Llm(format!(
                "claude CLI failed ({}): {}",
                out.status,
                String::from_utf8_lossy(&out.stderr).trim()
            )));
        }
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    }

    fn model_name(&self) -> &str {
        &self.model
    }
}

/// Compose the stdin prompt for `claude -p`: the system instruction inlined ahead of the user text.
/// The CLI's built-in agent system prompt overrides `--append-system-prompt`, so structured
/// instructions must live in the turn itself to be honoured. Empty system → user text unchanged.
fn compose_prompt(system: &str, user: &str) -> String {
    if system.trim().is_empty() {
        user.to_string()
    } else {
        format!("{system}\n\n{user}")
    }
}

#[cfg(test)]
mod tests {
    use super::compose_prompt;

    #[test]
    fn compose_inlines_system_before_user() {
        assert_eq!(
            compose_prompt("Return ONLY JSON.", "Alice likes tea."),
            "Return ONLY JSON.\n\nAlice likes tea."
        );
    }

    #[test]
    fn compose_passes_user_through_when_system_blank() {
        assert_eq!(compose_prompt("", "hello"), "hello");
        assert_eq!(compose_prompt("   ", "hello"), "hello");
    }
}
