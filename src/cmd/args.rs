use clap::{ArgAction, Args, Parser, Subcommand, ValueHint};

use crate::engine::vm::VmSourceType;

static LONG_ABOUT: &'static str = include_str!("./long.txt");

#[derive(Debug, Clone, Parser)]
#[clap(
    long_about = LONG_ABOUT,
    author,
    version,
    name = "filterx",
)]
pub struct Cli {
    #[clap(subcommand)]
    pub command: Command,
}

#[derive(Debug, Clone, Subcommand)]
pub enum Command {
    /// handle csv file
    #[command(visible_alias = "c")]
    Csv(CsvCommand),

    /// handle fasta file
    Fasta(FastaCommand),

    /// handle fastq file
    Fastq(FastqCommand),
}

#[derive(Debug, Clone, Args)]
pub struct ShareArgs {
    /// The input string
    #[clap(value_hint=ValueHint::FilePath)]
    pub input: String,

    /// expression to filter
    #[clap(short = 'e', long, action = ArgAction::Append)]
    pub expr: Option<Vec<String>>,

    /// The output file, default is stdout
    #[clap(short='o', long, value_hint=ValueHint::FilePath)]
    pub output: Option<String>,

    /// output as table format
    #[clap(short = 't', long, default_value = "false", action = ArgAction::SetTrue)]
    pub table: Option<bool>,
}

#[derive(Debug, Clone, Parser)]
pub struct CsvCommand {
    #[clap(flatten)]
    pub share_args: ShareArgs,

    /// whether the input file has header, default is false
    #[clap(short = 'H', long, default_value = "false", action = ArgAction::SetTrue)]
    pub header: Option<bool>,

    /// whether output header to file, default is false
    #[clap(long = "oH", default_value = "false", action = ArgAction::SetTrue)]
    pub output_header: Option<bool>,

    /// The comment prefix
    #[clap(short = 'c', long, default_value = "#")]
    pub comment_prefix: Option<String>,

    /// The separator
    #[clap(short = 's', long, default_value = ",")]
    pub separator: Option<String>,

    /// The output separator
    #[clap(long = "os", default_value = ",")]
    pub output_separator: Option<String>,

    /// skip row number, 0 means no skip
    #[clap(long, default_value = "0")]
    pub skip_row: Option<usize>,

    /// limit row number, 0 means no limit
    #[clap(long, default_value = "0")]
    pub limit_row: Option<usize>,

    /// scope of the filterx
    #[clap(long, default_value = "csv")]
    pub scope: Option<VmSourceType>,
}

#[derive(Debug, Clone, Parser)]
pub struct FastaCommand {
    #[clap(flatten)]
    pub share_args: ShareArgs,

    /// process fasta file by chunk
    #[clap(short = 'c', long)]
    pub chunk: Option<usize>,
}

#[derive(Debug, Clone, Parser)]
pub struct FastqCommand {}
