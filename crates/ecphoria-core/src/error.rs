use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("storage error: {0}")]
    Storage(String),

    #[error("query error: {0}")]
    Query(String),

    #[error("embedding error: {0}")]
    Embedding(String),

    #[error("llm error: {0}")]
    Llm(String),

    #[error("ingest error: {0}")]
    Ingest(String),

    #[error("state error: {0}")]
    State(String),

    #[error("configuration error: {0}")]
    Config(String),

    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display_storage() {
        let err = Error::Storage("disk full".into());
        assert_eq!(err.to_string(), "storage error: disk full");
    }

    #[test]
    fn error_display_query() {
        let err = Error::Query("syntax error at position 5".into());
        assert_eq!(err.to_string(), "query error: syntax error at position 5");
    }

    #[test]
    fn error_display_embedding() {
        let err = Error::Embedding("provider unavailable".into());
        assert_eq!(err.to_string(), "embedding error: provider unavailable");
    }

    #[test]
    fn error_display_ingest() {
        let err = Error::Ingest("invalid payload".into());
        assert_eq!(err.to_string(), "ingest error: invalid payload");
    }

    #[test]
    fn error_display_state() {
        let err = Error::State("version conflict".into());
        assert_eq!(err.to_string(), "state error: version conflict");
    }

    #[test]
    fn error_display_config() {
        let err = Error::Config("missing field 'data_dir'".into());
        assert_eq!(
            err.to_string(),
            "configuration error: missing field 'data_dir'"
        );
    }

    #[test]
    fn error_from_anyhow() {
        let anyhow_err = anyhow::anyhow!("something went wrong");
        let err: Error = anyhow_err.into();
        assert_eq!(err.to_string(), "something went wrong");
        assert!(matches!(err, Error::Internal(_)));
    }

    #[test]
    fn error_is_debug() {
        let err = Error::Storage("test".into());
        let debug = format!("{:?}", err);
        assert!(debug.contains("Storage"));
    }

    #[test]
    fn result_type_alias_works() {
        let ok: Result<i32> = Ok(42);
        assert!(ok.is_ok());
        match ok {
            Ok(v) => assert_eq!(v, 42),
            Err(_) => panic!("expected Ok"),
        }

        let err: Result<i32> = Err(Error::Query("fail".into()));
        assert!(err.is_err());
    }
}
