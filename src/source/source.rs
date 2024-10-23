use polars::prelude::*;

use super::{DataframeSource, FastaSource, FastqSource};
use crate::FilterxResult;
pub enum Source {
    Fasta(FastaSource),
    Fastq(FastqSource),
    Dataframe(DataframeSource),
}

#[derive(Debug, Clone, PartialEq)]
pub enum SourceType {
    Block,
    Dataframe,
}

impl Source {
    pub fn new_fasta(fasta: FastaSource) -> Self {
        Source::Fasta(fasta)
    }

    pub fn new_fastq(fastq: FastqSource) -> Self {
        Source::Fastq(fastq)
    }

    pub fn new_dataframe(dataframe: DataframeSource) -> Self {
        Source::Dataframe(dataframe)
    }
}

impl Source {
    pub fn finish(&mut self) -> FilterxResult<()> {
        match self {
            Source::Dataframe(df) => {
                let plan = df.lazy.describe_optimized_plan();
                if plan.is_err() {
                    return Ok(());
                }
                let plan = plan.unwrap();
                if plan.len() == 0 {
                    return Ok(());
                }
                let ret_df = df.lazy.clone().collect()?;
                df.update(ret_df.lazy());
            }
            _ => {}
        };
        Ok(())
    }

    pub fn dataframe_mut_ref(&mut self) -> Option<&mut DataframeSource> {
        match self {
            Source::Dataframe(df) => Some(df),
            _ => None,
        }
    }

    pub fn into_dataframe(self) -> Option<DataframeSource> {
        match self {
            Source::Dataframe(df) => Some(df),
            _ => None,
        }
    }

    pub fn fasta_mut_ref(&mut self) -> Option<&mut FastaSource> {
        match self {
            Source::Fasta(fasta) => Some(fasta),
            _ => None,
        }
    }

    pub fn into_fasta(self) -> Option<FastaSource> {
        match self {
            Source::Fasta(fasta) => Some(fasta),
            _ => None,
        }
    }

    pub fn fastq_mut_ref(&mut self) -> Option<&mut FastqSource> {
        match self {
            Source::Fastq(fastq) => Some(fastq),
            _ => None,
        }
    }

    pub fn into_fastq(self) -> Option<FastqSource> {
        match self {
            Source::Fastq(fastq) => Some(fastq),
            _ => None,
        }
    }

    pub fn source_type(&self) -> SourceType {
        match self {
            Source::Dataframe(_) => SourceType::Dataframe,
            _ => SourceType::Block,
        }
    }
}
