pub mod fasta;
pub mod fastq;

#[derive(Clone, Debug)]
pub enum FastaRecordType {
    DNA,
    RNA,
    PROTEIN,
    UNKNOWN,
}
