use polars::{
    io::{csv, SerWriter},
    prelude::{CsvParseOptions, CsvReadOptions, DataFrame, LazyFrame},
};
use std::io::Write;
use std::{fs::File, num::NonZero};

use super::args::{CsvCommand, ShareArgs};
use crate::engine::vm::Vm;
use crate::source::DataframeSource;

use crate::util;
use crate::FilterxResult;

fn init_df(
    path: &str,
    header: bool,
    comment_prefix: &str,
    separator: &str,
    skip_row: usize,
    limit_row: Option<usize>,
) -> FilterxResult<LazyFrame> {
    let parser_options = CsvParseOptions::default()
        .with_comment_prefix(Some(comment_prefix))
        .with_separator(util::handle_sep(separator) as u8);

    let parser_option = CsvReadOptions::default()
        .with_parse_options(parser_options)
        .with_has_header(header)
        .with_skip_rows(skip_row)
        .with_n_rows(limit_row)
        .with_schema(None);

    util::open_csv_file_in_lazy(path, parser_option)
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

pub fn filterx_csv(cmd: CsvCommand) -> FilterxResult<()> {
    let CsvCommand {
        share_args:
            ShareArgs {
                input: path,
                expr,
                output,
                table,
            },
        header,
        output_header,
        comment_prefix,
        separator,
        output_separator,
        skip_row,
        limit_row,
    } = cmd;

    let comment_prefix = comment_prefix.unwrap();
    let separator = separator.unwrap();
    let writer = util::create_buffer_writer(output.clone())?;
    let lazy_df = init_df(
        path.as_str(),
        header.unwrap(),
        &comment_prefix,
        &separator,
        skip_row.unwrap(),
        limit_row,
    )?;
    let mut s = DataframeSource::new(lazy_df.clone());
    s.set_has_header(header.unwrap());
    let mut vm = Vm::from_dataframe(s);
    if header.is_some() {
        let lazy = init_df(
            path.as_str(),
            header.unwrap(),
            &comment_prefix,
            &separator,
            skip_row.unwrap(),
            limit_row,
        )?;
        vm.status.inject_columns_by_df(lazy)?;
    }
    let expr = util::merge_expr(expr);
    let writer = Box::new(writer);
    vm.set_writer(writer);
    vm.eval_once(&expr)?;
    vm.finish()?;
    if vm.status.printed {
        return Ok(());
    }
    let mut df = vm.source.into_dataframe().unwrap().into_df()?;
    if output.is_none() && table.unwrap_or(false) {
        println!("{}", df);
        return Ok(());
    }
    if vm.status.printed {
        return Ok(());
    }
    write_df(
        &mut df,
        output.as_deref(),
        output_header.unwrap(),
        output_separator.unwrap().as_str(),
    )
}
