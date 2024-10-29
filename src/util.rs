use core::str;

use crate::engine::value;
use polars::prelude::*;

use crate::{FilterxError, FilterxResult};
use std::fs::File;
use std::io::BufWriter;
use std::io::Write;

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
    let lazy_reader = lazy_reader.with_has_header(reader_options.has_header);
    let lazy_reader = lazy_reader.with_skip_rows(reader_options.skip_rows);
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
