use anyhow::Result;
use thiserror::Error as ThisError;

pub type FilterxResult<T> = Result<T, FilterxError>;

#[derive(ThisError, Debug)]
pub enum FilterxError {
    #[error("EngineError: {0}")]
    EngineError(#[from] polars::prelude::PolarsError),

    #[error("Open file error: {0}")]
    IOError(#[from] std::io::Error),

    #[error("Runtime error: {0}")]
    RuntimeError(String),

    #[error("Parse error: {0}")]
    ParseError(#[from] rustpython_parser::ParseError),

    #[error("Error reading Fasta file: {0}")]
    FastaError(String),

    #[error("Error reading Fastq file: {0}")]
    FastqError(String),
}
