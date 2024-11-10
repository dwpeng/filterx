pub mod block;
pub mod dataframe;

pub use block::fasta::FastaSource;
pub use block::fastq::FastqSource;
pub use block::table_like::TableLike;
pub use dataframe::detect_columns;
pub use dataframe::Source;
