pub mod block;
pub mod dataframe;
pub mod source;

pub use block::fasta::{FastaRecordType, FastaSource};
pub use block::fastq::{FastqSource, QualityType};
pub use dataframe::detect_columns;
pub use dataframe::DataframeSource;
pub use source::{Source, SourceInner, SourceType};
