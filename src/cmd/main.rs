use std::io::Write;
use std::{fs::File, num::NonZero};

use clap::Parser;
use polars::{
    io::{csv, SerWriter},
    prelude::{CsvParseOptions, CsvReadOptions, DataFrame, LazyFrame},
};

use crate::engine::vm::Vm;
use crate::source::DataframeSource;
use crate::util;
use crate::{util::open_csv_file_mmaped, FilterxResult};

use super::args::FilterxCommand;

fn init_df(
    path: &str,
    header: bool,
    comment_prefix: &str,
    separator: &str,
    skip_row: usize,
) -> FilterxResult<LazyFrame> {
    let parser_options = CsvParseOptions::default()
        .with_comment_prefix(Some(comment_prefix))
        .with_separator(util::handle_sep(separator) as u8);

    let parser_option = CsvReadOptions::default()
        .with_parse_options(parser_options)
        .with_has_header(header)
        .with_skip_rows(skip_row)
        .with_schema(None);

    open_csv_file_mmaped(path, parser_option)
}

fn write_df(
    df: &mut DataFrame,
    output: Option<&str>,
    output_header: bool,
    output_separator: &str,
) -> FilterxResult<()> {
    let writer: Box<dyn Write>;
    if let Some(output) = output {
        writer = Box::new(File::create(output)?);
    } else {
        writer = Box::new(std::io::stdout());
    }
    let mut writer = csv::write::CsvWriter::new(writer)
        .include_header(output_header)
        .with_batch_size(NonZero::new(1000).unwrap())
        .with_separator(util::handle_sep(output_separator) as u8);
    writer.finish(df)?;
    Ok(())
}

fn init_engine(lazy: LazyFrame) -> FilterxResult<Vm> {
    let df_source = DataframeSource::new(lazy);
    let vm = Vm::from_dataframe(df_source);
    Ok(vm)
}

pub fn cli() -> FilterxResult<()> {
    let parser = FilterxCommand::parse();
    let FilterxCommand {
        csv_path: path,
        expr,
        output,
        header,
        output_header,
        comment_prefix,
        separator,
        output_separator,
        skip_row,
    } = parser;
    let lazy_df = init_df(
        path.as_str(),
        header.unwrap(),
        comment_prefix.unwrap().as_str(),
        separator.unwrap().as_str(),
        skip_row.unwrap(),
    )?;

    let mut vm = init_engine(lazy_df)?;
    let expr = expr.unwrap_or("".into());
    vm.eval(&expr)?;
    vm.finish()?;
    write_df(
        &mut vm.source.dataframe().unwrap().df.as_mut().unwrap(),
        output.as_deref(),
        output_header.unwrap(),
        output_separator.unwrap().as_str(),
    )
}
