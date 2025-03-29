use thiserror::Error;

#[derive(Debug, Error)]
pub enum AnyError {
    #[error("NotFound({0})")]
    NotFound(String),
    #[error("AlreadyExists({0})")]
    AlreadyExists(String),
    #[error("ParseError({0})")]
    ParseError(String),
    #[error("InvalidParameter({0})")]
    InvalidParameter(String),
    #[error("TimeoutError({0})")]
    TimeoutError(String),
    #[error("ExternalServiceError({0})")]
    ExternalServiceError(String),
    #[error("RuntimeError({0})")]
    RuntimeError(String),
    #[error("UnknownError({0})")]
    UnknownError(String),
}
