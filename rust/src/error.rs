use thiserror::Error;

#[derive(Debug, Error)]
pub enum ScantivyError {
    #[error("index not found: {0}")]
    IndexNotFound(String),

    #[error("missing field: {0}")]
    MissingField(String),

    #[error("invalid value for field '{field}': {reason}")]
    InvalidValue { field: String, reason: String },

    #[error("unsupported field type for operation: {0}")]
    UnsupportedFieldType(String),

    #[error("unsupported query node: {0}")]
    UnsupportedQuery(String),

    #[error("schema mismatch: {0}")]
    SchemaMismatch(String),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("decode: {0}")]
    Decode(#[from] prost::DecodeError),

    #[error("encode: {0}")]
    Encode(#[from] prost::EncodeError),

    #[error("tantivy: {0}")]
    Tantivy(String),
}

impl From<tantivy::TantivyError> for ScantivyError {
    fn from(err: tantivy::TantivyError) -> Self {
        ScantivyError::Tantivy(err.to_string())
    }
}

impl From<tantivy::query::QueryParserError> for ScantivyError {
    fn from(err: tantivy::query::QueryParserError) -> Self {
        ScantivyError::Tantivy(err.to_string())
    }
}

impl From<tantivy::directory::error::OpenDirectoryError> for ScantivyError {
    fn from(err: tantivy::directory::error::OpenDirectoryError) -> Self {
        ScantivyError::Tantivy(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, ScantivyError>;
