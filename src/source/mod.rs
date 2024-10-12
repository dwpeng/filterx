mod block;
mod dataframe;

pub use block::fasta::Fasta as FastaSource;
pub use block::fastq::Fastq as FastqSource;
pub use block::record::Filter;
pub use block::table_like::TableLike;
pub use dataframe::DataframeSource;
use polars::prelude::IntoLazy;

use crate::FilterxResult;

pub enum Source {
    Fasta(FastaSource),
    Fastq(FastqSource),
    Dataframe(DataframeSource),
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
                let ret_df = df.lazy.clone().collect()?;
                df.df = Some(ret_df);
                df.lazy = df.df.clone().expect("Dataframe is None").lazy();
            }
            _ => {}
        };
        Ok(())
    }

    pub fn dataframe(&mut self) -> Option<&mut DataframeSource> {
        match self {
            Source::Dataframe(df) => Some(df),
            _ => None,
        }
    }

    pub fn fasta(&mut self) -> Option<&mut FastaSource> {
        match self {
            Source::Fasta(fasta) => Some(fasta),
            _ => None,
        }
    }

    pub fn fastq(&mut self) -> Option<&mut FastqSource> {
        match self {
            Source::Fastq(fastq) => Some(fastq),
            _ => None,
        }
    }
}
