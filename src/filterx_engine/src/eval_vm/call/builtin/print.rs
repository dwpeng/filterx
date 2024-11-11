use polars::{
    frame::DataFrame,
    prelude::{format_str, IntoLazy},
};

use super::*;
use polars::prelude::{col, Expr};

use regex::Regex;
use std::io::Write;

use lazy_static::lazy_static;

lazy_static! {
    static ref REGEX_PATTERN: Regex = Regex::new(r"\{([\(\)a-zA-Z0-9_\-+*\\]*)\}").unwrap();
    static ref REGEX_VARNAME: Regex = Regex::new(r"^[_a-zA-Z]+[a-zA-Z_0-9]*$").unwrap();
}

fn parse_format_string(
    valid_names: Option<&Vec<String>>,
    s: &str,
) -> FilterxResult<(String, Option<Vec<Expr>>)> {
    // value: "xxxxx"  ->  "xxxxx"
    // value: "xxx_{seq}" ->  "xxx_{}" and col("seq")
    // value: "xxx_{seq}_{seq}" -> "xxx_{}_{}" and col("seq"), col("seq")
    // value: "xxx{len(seq)} -> "xxx{}" and len(col("seq"))
    if s.is_empty() {
        return Err(FilterxError::RuntimeError(
            "Error format string, empty format string".to_string(),
        ));
    }

    if !s.contains("{") {
        return Ok((s.to_string(), None));
    }

    let re = &REGEX_PATTERN;
    let fmt = re.replace_all(s, "{}").to_string();
    let mut cols = Vec::new();
    let mut vm = Vm::from_dataframe(Source::new(DataFrame::empty().lazy()));
    if let Some(valid_names) = valid_names {
        vm.source.set_init_column_names(valid_names);
    }
    for cap in re.captures_iter(s) {
        let item = cap.get(1).unwrap().as_str();
        if item.is_empty() {
            return Err(FilterxError::RuntimeError(
                "Error format string, empty format value".to_string(),
            ));
        }
        if REGEX_VARNAME.is_match(item) {
            // recheck columns name
            vm.source.has_column(item);
            cols.push(col(item));
            continue;
        }
        let ast = vm.ast(item)?;
        if !ast.is_expression() {
            return Err(FilterxError::RuntimeError(
                "Error format string, only support expression".to_string(),
            ));
        }
        let ast = ast.expression().unwrap();
        let ast = ast.eval(&mut vm)?;
        let value = ast.expr()?;
        cols.push(value);
    }
    Ok((fmt, Some(cols)))
}

#[test]
fn test_parse_format_string() {
    use polars::prelude::col;

    let s = "xxx_{seq}";
    let (fmt, cols) = parse_format_string(None, s).unwrap();
    assert_eq!(fmt, "xxx_{}");
    assert!(cols.is_some());
    let cols = cols.unwrap();
    assert_eq!(cols.len(), 1);
    assert_eq!(cols[0], col("seq"));

    let s = "xxx_{seq}_{seq}";
    let (fmt, cols) = parse_format_string(None, s).unwrap();
    assert_eq!(fmt, "xxx_{}_{}");
    assert!(cols.is_some());
    let cols = cols.unwrap();
    assert_eq!(cols.len(), 2);
    assert_eq!(cols[0], col("seq"));
    assert_eq!(cols[1], col("seq"));

    let s = "xxx";
    let (fmt, cols) = parse_format_string(None, s).unwrap();
    assert_eq!(fmt, "xxx");
    assert!(cols.is_none());

    let s = "xxx{len(seq)}";
    let (fmt, cols) = parse_format_string(None, s).unwrap();
    assert_eq!(fmt, "xxx{}");
    assert!(cols.is_some());
    let cols = cols.unwrap();
    assert_eq!(cols.len(), 1);
    assert_eq!(cols[0], col("seq").str().len_chars());
}

const FORMAT_COLUMN_NAME: &str = "__@#$fmt__";

pub fn print<'a>(vm: &'a mut Vm, args: &Vec<ast::Expr>) -> FilterxResult<value::Value> {
    expect_args_len(args, 1)?;
    if vm.status.consume_rows >= vm.status.limit_rows {
        return Ok(value::Value::None);
    }
    let value = eval!(
        vm,
        &args[0],
        "print: expected a string literal or format string expression as first argument",
        Constant
    );
    let value = value.text()?;
    // get from cache
    let fmt;
    let cols;
    if let Some(value) = vm.expr_cache.get(&value) {
        fmt = &value.0;
        cols = &value.1;
    } else {
        let (fmt_, cols_) = parse_format_string(Some(&vm.source.ret_column_names), &value)?;
        let cols_ = cols_.unwrap_or(vec![]);
        vm.expr_cache
            .insert(value.clone(), (fmt_.clone(), cols_.clone()));
        let value = vm.expr_cache.get(&value).unwrap();
        fmt = &value.0;
        cols = &value.1;
    }
    let lazy = vm.source.lazy().select([format_str(&fmt, &cols)?.alias(FORMAT_COLUMN_NAME)]);
    vm.source.update(lazy);
    let df = vm.source.lazy().collect()?;
    let fmt = df.column(FORMAT_COLUMN_NAME).unwrap();
    let writer = vm.writer.as_mut().unwrap().as_mut();
    let need = vm.status.limit_rows - vm.status.consume_rows;
    let fmt = fmt.slice(0, usize::min(need, fmt.len()));
    for i in 0..fmt.len() {
        let value = fmt.get(i)?;
        let s = value.get_str().unwrap_or("");
        writeln!(writer, "{}", s)?;
    }
    vm.status.consume_rows += fmt.len();
    vm.status.printed = true;
    Ok(value::Value::None)
}
