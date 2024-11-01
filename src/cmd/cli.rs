use super::args::{Cli, Command};
use super::csv::filterx_csv;
use super::fasta::filterx_fasta;
use super::fastq::filterx_fastq;
use super::gxf::filterx_gxf;
use super::sam::filterx_sam;
use super::vcf::filterx_vcf;
use crate::FilterxResult;

use clap::Parser;

pub fn cli() -> FilterxResult<()> {
    let parser = Cli::parse();
    match parser.command {
        Command::Csv(cmd) => filterx_csv(cmd),
        Command::Fasta(cmd) => filterx_fasta(cmd),
        Command::Fastq(cmd) => filterx_fastq(cmd),
        Command::Sam(cmd) => filterx_sam(cmd),
        Command::Vcf(cmd) => filterx_vcf(cmd),
        Command::GFF(cmd) => filterx_gxf(cmd),
        Command::GTF(cmd) => filterx_gxf(cmd),
    }
}
