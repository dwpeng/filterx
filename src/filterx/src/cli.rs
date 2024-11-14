use crate::args::{Cli, Command};
use crate::files::csv::filterx_csv;
use crate::files::fasta::filterx_fasta;
use crate::files::fastq::filterx_fastq;
use crate::files::gxf::{filterx_gxf, GxfType};
use crate::files::sam::filterx_sam;
use crate::files::vcf::filterx_vcf;

use clap::Parser;

use filterx_core::FilterxResult;

pub fn cli() -> FilterxResult<()> {
    let parser = Cli::parse();
    match parser.command {
        Command::Csv(cmd) => filterx_csv(cmd),
        Command::Fasta(cmd) => filterx_fasta(cmd),
        Command::Fastq(cmd) => filterx_fastq(cmd),
        Command::Sam(cmd) => filterx_sam(cmd),
        Command::Vcf(cmd) => filterx_vcf(cmd),
        Command::GFF(cmd) => filterx_gxf(cmd, GxfType::Gff),
        Command::GTF(cmd) => filterx_gxf(cmd, GxfType::Gtf),
    }
}
