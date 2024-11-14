pub mod block;
pub mod dataframe;
pub mod source;

pub use block::fasta::FastaSource;
pub use block::fastq::FastqSource;
pub use dataframe::detect_columns;
pub use dataframe::DataframeSource;
pub use source::{Source, SourceInner, SourceType};
