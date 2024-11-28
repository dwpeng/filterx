use core::str;

use polars::{
    io::{csv, SerWriter},
    prelude::*,
};

use crate::{
    sep::Separator, thread_size::ThreadSize, writer::FilterxWriter, FilterxError, FilterxResult,
};
use std::io::Write;
use std::num::NonZero;

pub fn open_csv_file_in_lazy(
    path: &str,
    reader_options: CsvReadOptions,
) -> FilterxResult<LazyFrame> {
    let path = std::path::Path::new(path);
    if path.exists() == false {
        return Err(FilterxError::RuntimeError(format!(
            "{} not exists",
            path.display()
        )));
    }
    let lazy_reader = LazyCsvReader::new(path);
    let comment_prefix = match reader_options.parse_options.comment_prefix.clone() {
        Some(prefix) => match prefix {
            CommentPrefix::Multi(prefix) => prefix.into_string(),
            CommentPrefix::Single(prefix) => str::from_utf8(&[prefix]).unwrap().to_string(),
        },
        None => "".to_string(),
    };
    let comment_prefix = match comment_prefix.len() {
        0 => None,
        _ => Some(comment_prefix.into()),
    };
    let lazy_reader = lazy_reader.with_comment_prefix(comment_prefix);
    let lazy_reader = lazy_reader.with_separator(reader_options.parse_options.separator);
    let lazy_reader =
        lazy_reader.with_null_values(reader_options.parse_options.null_values.clone());
    let lazy_reader = lazy_reader.with_has_header(reader_options.has_header);
    let lazy_reader = lazy_reader.with_skip_rows(reader_options.skip_rows);
    let lazy_reader = lazy_reader.with_n_rows(reader_options.n_rows);
    let lazy_reader = lazy_reader.with_infer_schema_length(reader_options.infer_schema_length);
    let lazy_reader = lazy_reader.with_schema(reader_options.schema);
    let lazy = lazy_reader.finish()?;
    Ok(lazy)
}

#[inline]
pub fn merge_expr(expr: Option<Vec<String>>) -> String {
    match expr {
        Some(expr) => expr.join(";"),
        None => "".to_string(),
    }
}

pub fn create_schemas(fileds: Vec<(String, DataType)>) -> Option<SchemaRef> {
    let mut schema = Schema::with_capacity(fileds.len());
    for (name, dtype) in fileds {
        // TODO: return None although insert successed
        let _r = schema.insert(name.into(), dtype.clone());
    }
    Some(Arc::new(schema))
}

pub fn init_df(
    path: &str,
    header: bool,
    comment_prefix: &str,
    separator: &str,
    skip_row: usize,
    limit_row: Option<usize>,
    schema: Option<SchemaRef>,
    null_values: Option<Vec<&str>>,
    missing_is_null: bool,
) -> FilterxResult<LazyFrame> {
    let sep = Separator::new(separator);
    let mut parser_options = CsvParseOptions::default()
        .with_comment_prefix(Some(comment_prefix))
        .with_separator(sep.get_sep()?)
        .with_truncate_ragged_lines(true);

    parser_options = parser_options.with_missing_is_null(missing_is_null);
    if let Some(null_values) = null_values {
        let null_values = NullValues::AllColumns(null_values.iter().map(|x| (*x).into()).collect());
        parser_options = parser_options.with_null_values(Some(null_values));
    }

    let mut read_options = CsvReadOptions::default()
        .with_parse_options(parser_options)
        .with_has_header(header)
        .with_skip_rows(skip_row)
        .with_n_rows(limit_row);

    if schema.is_some() {
        read_options = read_options.with_schema(schema);
    } else {
        read_options = read_options.with_infer_schema_length(Some(1000));
    }

    let lazy = open_csv_file_in_lazy(path, read_options);

    lazy
}

pub fn write_df(
    df: &mut DataFrame,
    writer: &mut FilterxWriter,
    output_header: bool,
    output_separator: Option<&str>,
    headers: Option<Vec<String>>,
    null_value: Option<&str>,
) -> FilterxResult<()> {
    if headers.is_some() {
        let headers = headers.unwrap();
        for line in headers {
            writer.write_all(line.as_bytes())?;
        }
    }
    let mut writer = csv::write::CsvWriter::new(writer)
        .include_header(output_header)
        .with_batch_size(NonZero::new(1024).unwrap())
        .with_quote_style(QuoteStyle::Never)
        .with_float_precision(Some(3))
        .n_threads(ThreadSize::get())
        .with_line_terminator("\n".into());

    if let Some(output_separator) = output_separator {
        writer = writer.with_separator(Separator::new(output_separator).get_sep()?);
    }

    if let Some(null_value) = null_value {
        writer = writer.with_null_value(null_value.into());
    }
    writer.finish(df)?;
    Ok(())
}

pub fn collect_comment_lines(path: &str, comment_prefix: &str) -> FilterxResult<Vec<String>> {
    use std::fs::File;
    use std::io::BufRead;
    use std::io::BufReader;
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut line = String::new();
    let mut comment_lines = Vec::new();
    loop {
        reader.read_line(&mut line)?;
        if line.starts_with(comment_prefix) {
            comment_lines.push(line.clone());
            line.clear();
            continue;
        }
        break;
    }
    Ok(comment_lines)
}
