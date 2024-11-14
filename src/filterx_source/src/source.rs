use crate::block::fasta::{Fasta, FastaSource};
use crate::block::fastq::{Fastq, FastqSource};
use crate::DataframeSource;

use filterx_core::{FilterxError, FilterxResult};
use polars::prelude::*;

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum SourceType {
    Csv,
    Fasta,
    Fastq,
    Vcf,
    Sam,
    Gff,
    Gtf,
}

impl SourceType {
    pub fn is_fasta(&self) -> bool {
        match self {
            SourceType::Fasta => true,
            _ => false,
        }
    }
    pub fn is_fastq(&self) -> bool {
        match self {
            SourceType::Fastq => true,
            _ => false,
        }
    }
    pub fn is_vcf(&self) -> bool {
        match self {
            SourceType::Vcf => true,
            _ => false,
        }
    }
    pub fn is_gff(&self) -> bool {
        match self {
            SourceType::Gff => true,
            _ => false,
        }
    }
    pub fn is_gtf(&self) -> bool {
        match self {
            SourceType::Gtf => true,
            _ => false,
        }
    }
    pub fn is_sam(&self) -> bool {
        match self {
            SourceType::Sam => true,
            _ => false,
        }
    }
}

impl Into<&str> for SourceType {
    fn into(self) -> &'static str {
        match self {
            SourceType::Csv => "csv",
            SourceType::Fasta => "fasta",
            SourceType::Fastq => "fastq",
            SourceType::Vcf => "vcf",
            SourceType::Sam => "sam",
            SourceType::Gff => "gff",
            SourceType::Gtf => "gtf",
        }
    }
}

impl From<&str> for SourceType {
    fn from(s: &str) -> Self {
        match s {
            "csv" => SourceType::Csv,
            "fasta" => SourceType::Fasta,
            "fastq" => SourceType::Fastq,
            "vcf" => SourceType::Vcf,
            "sam" => SourceType::Sam,
            "gff" => SourceType::Gff,
            "gtf" => SourceType::Gtf,
            _ => panic!("Invalid source type"),
        }
    }
}

pub enum SourceInner {
    DataFrame(DataframeSource),
    Fasta(FastaSource),
    Fastq(FastqSource),
}

impl From<DataframeSource> for SourceInner {
    fn from(df: DataframeSource) -> Self {
        SourceInner::DataFrame(df)
    }
}

impl From<FastaSource> for SourceInner {
    fn from(fasta: FastaSource) -> Self {
        SourceInner::Fasta(fasta)
    }
}

impl From<FastqSource> for SourceInner {
    fn from(fastq: FastqSource) -> Self {
        SourceInner::Fastq(fastq)
    }
}

pub struct Source {
    pub source_type: SourceType,
    pub inner: SourceInner,
}

impl Source {
    pub fn new(inner: SourceInner, source_type: SourceType) -> Self {
        Source { source_type, inner }
    }
}

impl Source {
    pub fn df_source_mut(&mut self) -> &mut DataframeSource {
        match &mut self.inner {
            SourceInner::DataFrame(df) => df,
            SourceInner::Fasta(fasta) => &mut fasta.dataframe,
            SourceInner::Fastq(fastq) => &mut fastq.dataframe,
        }
    }

    pub fn df_source(&self) -> &DataframeSource {
        match &self.inner {
            SourceInner::DataFrame(df) => df,
            SourceInner::Fasta(fasta) => &fasta.dataframe,
            SourceInner::Fastq(fastq) => &fastq.dataframe,
        }
    }

    pub fn into_df(&self) -> FilterxResult<DataFrame> {
        let s = match &self.inner {
            SourceInner::DataFrame(df) => df,
            SourceInner::Fasta(ref fasta) => &fasta.dataframe,
            SourceInner::Fastq(ref fastq) => &fastq.dataframe,
        };
        let df = s.lazy();
        Ok(df.collect()?)
    }

    pub fn get_fastq(&self) -> FilterxResult<&Fastq> {
        match &self.inner {
            SourceInner::Fastq(fastq) => Ok(&fastq.fastq),
            _ => Err(FilterxError::RuntimeError(
                "get_fastq only support Fastq source".into(),
            )),
        }
    }

    pub fn get_fasta(&self) -> FilterxResult<&Fasta> {
        match &self.inner {
            SourceInner::Fasta(fasta) => Ok(&fasta.fasta),
            _ => Err(FilterxError::RuntimeError(
                "get_fasta only support Fasta source".into(),
            )),
        }
    }
}
