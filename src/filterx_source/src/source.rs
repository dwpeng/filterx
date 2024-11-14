use crate::block::fasta::FastaSource;
use crate::block::fastq::FastqSource;
use crate::DataframeSource;

use filterx_core::FilterxResult;
use polars::prelude::*;

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum SourceType {
    Csv,
    Fasta,
    Fastq,
    Vcf,
    Sam,
    Gxf,
}

impl Into<&str> for SourceType {
    fn into(self) -> &'static str {
        match self {
            SourceType::Csv => "csv",
            SourceType::Fasta => "fasta",
            SourceType::Fastq => "fastq",
            SourceType::Vcf => "vcf",
            SourceType::Sam => "sam",
            SourceType::Gxf => "gxf",
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
            "gxf" => SourceType::Gxf,
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
}
