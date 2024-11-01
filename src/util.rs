use core::str;

use crate::engine::value;
use polars::{
    io::{csv, SerWriter},
    prelude::*,
};

use crate::{FilterxError, FilterxResult};
use std::io::BufWriter;
use std::io::Write;
use std::{fs::File, num::NonZero};

pub fn open_csv_file(path: &str, reader_options: CsvReadOptions) -> FilterxResult<DataFrame> {
    let path = std::path::Path::new(path);
    if path.exists() == false {
        return Err(FilterxError::RuntimeError(format!(
            "{} not exists",
            path.display()
        )));
    }
    let reader = reader_options.try_into_reader_with_file_path(Some(path.into()))?;
    let df = reader.finish()?;
    Ok(df)
}

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

static SEPARATORS: [(&str, char); 4] = [("\\t", '\t'), ("\\n", '\n'), ("\\r", '\r'), ("\\s", ' ')];
static FILE_SPE: char = '@';

pub fn handle_sep(sep: &str) -> char {
    match sep {
        s if s.starts_with("\\") => {
            for (s1, s2) in SEPARATORS.iter() {
                if s == *s1 {
                    return *s2;
                }
            }
            panic!("unsupported type");
        }
        _ => sep.chars().next().unwrap(),
    }
}

pub fn handle_file(path_repr: &str) -> FilterxResult<value::Value> {
    let path_repr = path_repr.to_string();
    let path_repr_list = path_repr.split(FILE_SPE).collect::<Vec<&str>>();
    let path = std::path::Path::new(path_repr_list[0]);
    let _ = std::fs::File::open(path)?;
    let mut file = value::File::default();
    file.file_name = path_repr_list[0].to_string();
    match path_repr_list.len() {
        2 => {
            file.select = path_repr_list[1].to_string();
        }
        3 => {
            file.select = path_repr_list[1].to_string();
            file.seprarator = handle_sep(path_repr_list[2]);
        }
        _ => {
            file.select = "1".to_string();
        }
    }
    let parser_options =
        polars::io::csv::read::CsvParseOptions::default().with_separator(file.seprarator as u8);
    let reader_options = polars::io::csv::read::CsvReadOptions::default()
        .with_parse_options(parser_options)
        .with_has_header(false);
    let df = reader_options.try_into_reader_with_file_path(Some(path.into()))?;
    let df = df.finish()?;
    let columns = df.get_column_names();
    match file.select.parse::<usize>() {
        Ok(mut i) => {
            i -= 1;
            file.select = columns[i].clone().into_string();
        }
        Err(_) => {}
    }
    file.df = df;
    Ok(value::Value::File(file))
}

pub fn mock_lazy_df() -> LazyFrame {
    let df = DataFrame::new(vec![
        Series::new("a".into(), &[1, 2, 3]),
        Series::new("b".into(), &[2, 3, 4]),
        Series::new("c".into(), &["a", "b", "c"]),
    ])
    .unwrap();
    df.lazy()
}

pub fn create_buffer_writer(path: Option<String>) -> FilterxResult<BufWriter<Box<dyn Write>>> {
    let writer: Box<dyn Write>;
    if let Some(path) = path {
        writer = Box::new(File::create(path)?);
    } else {
        writer = Box::new(std::io::stdout());
    }
    Ok(BufWriter::new(writer))
}

#[inline]
pub fn merge_expr(expr: Option<Vec<String>>) -> String {
    match expr {
        Some(expr) => expr.join(";"),
        None => "".to_string(),
    }
}

#[inline(always)]
pub fn append_vec<T: Copy>(dst: &mut Vec<T>, src: &[T]) {
    if src.len() + dst.len() > dst.capacity() {
        dst.reserve(src.len());
    }
    dst.extend_from_slice(src);
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
    let mut parser_options = CsvParseOptions::default()
        .with_comment_prefix(Some(comment_prefix))
        .with_separator(handle_sep(separator) as u8)
        .with_missing_is_null(missing_is_null);

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
        read_options = read_options.with_infer_schema_length(Some(0));
        read_options = read_options.with_schema(schema);
    }

    let lazy = open_csv_file_in_lazy(path, read_options);

    lazy
}

pub fn write_df(
    df: &mut DataFrame,
    output: Option<&str>,
    output_header: bool,
    output_separator: &str,
    headers: Option<Vec<String>>,
    null_value: Option<&str>,
) -> FilterxResult<()> {
    let mut writer: Box<dyn Write>;
    if let Some(output) = output {
        writer = Box::new(File::create(output)?);
    } else {
        writer = Box::new(std::io::stdout());
    }
    if headers.is_some() {
        let headers = headers.unwrap();
        for line in headers {
            writer.write_all(line.as_bytes())?;
        }
    }
    let mut writer = csv::write::CsvWriter::new(writer)
        .include_header(output_header)
        .with_batch_size(NonZero::new(1024).unwrap())
        .with_separator(handle_sep(output_separator) as u8)
        .with_quote_style(QuoteStyle::Never)
        .with_float_precision(Some(3))
        .n_threads(4)
        .with_line_terminator("\n".into());
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
