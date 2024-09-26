use clap::*;

static LONG_ABOUT: &'static str = include_str!("./long.txt");

#[derive(Debug, Clone, Parser)]
#[clap(
    long_about = LONG_ABOUT,
    author = "dwpeng",
    version = "0.1.0"
)]
pub struct FilterxCommand {
    /// The path of the csv file
    #[clap(value_hint=ValueHint::FilePath)]
    pub csv_path: String,

    /// The expression to filter
    #[clap()]
    pub expr: Option<String>,

    /// The output file, default is stdout
    #[clap(short = 'o', long, value_hint=ValueHint::FilePath)]
    pub output: Option<String>,

    /// whether the input file has header, default is false
    #[clap(short = 'H', long, default_value = "false", action = ArgAction::SetTrue)]
    pub header: Option<bool>,

    /// whether output header to file, default is true
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

    /// skip row number
    #[clap(long, default_value = "0")]
    pub skip_row: Option<usize>,
}
