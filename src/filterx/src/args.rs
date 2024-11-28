use clap::{ArgAction, Args, Parser, Subcommand, ValueHint};
use filterx_core::reader::FileContentType;
use filterx_source::{FastaRecordType, QualityType};

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

    /// Set the number of threads to use. Defaults to the number of logical CPUs.
    #[clap(short = 'j', long)]
    pub threads: Option<usize>,
}

#[derive(Debug, Clone, Subcommand)]
pub enum Command {
    /// handle csv file
    #[command(visible_alias = "c")]
    Csv(CsvCommand),

    /// handle fasta file
    #[command(visible_alias = "fa")]
    Fasta(FastaCommand),

    /// handle fastq file
    #[command(visible_alias = "fq")]
    Fastq(FastqCommand),

    /// handle sam file
    Sam(SamCommand),

    /// handle vcf file
    Vcf(VcfCommand),

    /// handle gff file
    GFF(GFFCommand),

    /// handle gtf file
    GTF(GFFCommand),

    /// builtin function help
    Info(InfoArgs),
}

pub fn set_thread_size(thread_size: Option<usize>) -> () {
    if thread_size.is_some() {
        let thread_size = thread_size.unwrap();
        // set polars thread size by env
        std::env::set_var("POLARS_NUM_THREADS", thread_size.to_string());
        // set gzp thread size by env
        std::env::set_var("GZP_NUM_THREADS", thread_size.to_string());
    }
}

#[derive(Debug, Clone, Args)]
pub struct ShareArgs {
    /// The input string
    #[clap(value_hint=ValueHint::FilePath)]
    pub input: String,

    /// expression to filter
    #[clap(short = 'e', long, action = ArgAction::Append)]
    pub expr: Option<Vec<String>>,

    /// The output file, default is stdout.
    #[clap(short='o', long, value_hint=ValueHint::FilePath)]
    pub output: Option<String>,

    /// output as table format, only output to stdout
    #[clap(short = 't', long, default_value = "false", action = ArgAction::SetTrue)]
    pub table: Option<bool>,

    /// only works with -o.
    #[clap(long, alias = "ot", default_value = "auto")]
    pub output_type: Option<FileContentType>,
}

#[derive(Debug, Clone, Parser)]
pub struct CsvCommand {
    #[clap(flatten)]
    pub share_args: ShareArgs,

    /// whether the input file has header, default is false
    #[clap(short = 'H', long, default_value = "false", action = ArgAction::SetTrue)]
    pub header: Option<bool>,

    /// Output headers if -H was set. --no-header will disable it.
    #[clap(long = "no-header", action = ArgAction::SetTrue, alias="nh")]
    pub no_header: Option<bool>,

    /// The comment prefix
    #[clap(short = 'c', long, default_value = "#")]
    pub comment_prefix: Option<String>,

    /// The separator
    #[clap(short = 's', long, default_value = ",")]
    pub separator: Option<String>,

    /// The output separator, same as -s if not set
    #[clap(long = "os")]
    pub output_separator: Option<String>,

    /// skip row number, 0 means no skip
    #[clap(long, default_value = "0")]
    pub skip: Option<usize>,

    /// limit row number, 0 means no limit
    #[clap(long, default_value = "0")]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Parser)]
pub struct FastaCommand {
    #[clap(flatten)]
    pub share_args: ShareArgs,

    /// number of sequence per chunk
    #[clap(short = 'c', long, default_value = "4096")]
    pub chunk: Option<usize>,

    /// don't parse comment
    #[clap(long, default_value = "false", action = ArgAction::SetTrue)]
    pub no_comment: Option<bool>,

    /// limit sequence number, 0 means no limit
    #[clap(long, default_value = "0")]
    pub limit: Option<usize>,

    /// sequence type, default is DNA
    #[clap(long, default_value = "auto")]
    pub r#type: Option<FastaRecordType>,

    /// detect sequence type by first N sequences
    #[clap(long, default_value = "3")]
    pub detect_size: Option<usize>,
}

#[derive(Debug, Clone, Parser)]
pub struct FastqCommand {
    #[clap(flatten)]
    pub share_args: ShareArgs,

    /// number of sequence per chunk
    #[clap(short = 'c', long, default_value = "4096")]
    pub chunk: Option<usize>,

    /// don't parse quality
    #[clap(long, default_value = "false", action = ArgAction::SetTrue, visible_alias="no-qual")]
    pub no_quality: Option<bool>,

    /// don't parse comment
    #[clap(long, default_value = "false", action = ArgAction::SetTrue)]
    pub no_comment: Option<bool>,

    /// limit sequence number, 0 means no limit
    #[clap(long, default_value = "0")]
    pub limit: Option<usize>,

    /// quality type, phred33, phred64, auto, auto: will try to detect
    #[clap(long, default_value = "auto")]
    pub phred: Option<QualityType>,

    /// detect quality type by first N sequences
    #[clap(long, default_value = "100")]
    pub detect_size: Option<usize>,
}

#[derive(Debug, Clone, Parser)]
pub struct SamCommand {
    #[clap(flatten)]
    pub share_args: ShareArgs,

    #[clap(short = 'H', long, default_value = "false", action = ArgAction::SetTrue)]
    pub header: Option<bool>,
}

#[derive(Debug, Clone, Parser)]
pub struct VcfCommand {
    #[clap(flatten)]
    pub share_args: ShareArgs,

    #[clap(short = 'H', long, default_value = "false", action = ArgAction::SetTrue)]
    pub header: Option<bool>,
}

#[derive(Debug, Clone, Parser)]
pub struct GFFCommand {
    #[clap(flatten)]
    pub share_args: ShareArgs,

    #[clap(short = 'H', long, default_value = "false", action = ArgAction::SetTrue)]
    pub header: Option<bool>,
}

#[derive(Debug, Clone, Parser)]
pub struct InfoArgs {
    /// builtin function name
    pub name: Option<String>,

    /// list all builtin functions
    #[clap(short='l', long, default_value = "false", action = ArgAction::SetTrue)]
    pub list: Option<bool>,
}
