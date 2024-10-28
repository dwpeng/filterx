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

fn parse_format_string(s: &str) -> FilterxResult<(String, Option<Vec<Expr>>)> {
    // value: "xxxxx"  ->  "xxxxx"
    // value: "xxx_{seq}" ->  "xxx_{}" and col("seq")
    // value: "xxx_{seq}_{seq}" -> "xxx_{}_{}" and col("seq"), col("seq")
    // value: "xxx{len(seq)} -> "xxx{}" and len(col("seq"))

    if s.is_empty() {
        return Err(FilterxError::RuntimeError(
            "Empty format string".to_string(),
        ));
    }

    if !s.contains("{") {
        return Ok((s.to_string(), None));
    }

    let re = &REGEX_PATTERN;
    let fmt = re.replace_all(s, "{}").to_string();
    let mut cols = Vec::new();
    let mut vm = Vm::from_dataframe(DataframeSource::new(DataFrame::empty().lazy()));
    for cap in re.captures_iter(s) {
        let item = cap.get(1).unwrap().as_str();
        if item.is_empty() {
            return Err(FilterxError::RuntimeError(
                "Error format string, empty format value".to_string(),
            ));
        }
        if REGEX_VARNAME.is_match(item) {
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
    let (fmt, cols) = parse_format_string(s).unwrap();
    assert_eq!(fmt, "xxx_{}");
    assert!(cols.is_some());
    let cols = cols.unwrap();
    assert_eq!(cols.len(), 1);
    assert_eq!(cols[0], col("seq"));

    let s = "xxx_{seq}_{seq}";
    let (fmt, cols) = parse_format_string(s).unwrap();
    assert_eq!(fmt, "xxx_{}_{}");
    assert!(cols.is_some());
    let cols = cols.unwrap();
    assert_eq!(cols.len(), 2);
    assert_eq!(cols[0], col("seq"));
    assert_eq!(cols[1], col("seq"));

    let s = "xxx";
    let (fmt, cols) = parse_format_string(s).unwrap();
    assert_eq!(fmt, "xxx");
    assert!(cols.is_none());

    let s = "xxx{len(seq)}";
    let (fmt, cols) = parse_format_string(s).unwrap();
    assert_eq!(fmt, "xxx{}");
    assert!(cols.is_some());
    let cols = cols.unwrap();
    assert_eq!(cols.len(), 1);
    assert_eq!(cols[0], col("seq").str().len_chars());
}

pub fn print<'a>(vm: &'a mut Vm, args: &Vec<ast::Expr>) -> FilterxResult<value::Value> {
    expect_args_len(args, 1)?;

    let value = eval!(
        vm,
        &args[0],
        "Only support string literal or format string expression",
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
        let (fmt_, cols_) = parse_format_string(&value)?;
        let cols_ = cols_.unwrap_or(vec![]);
        vm.expr_cache
            .insert(value.clone(), (fmt_.clone(), cols_.clone()));
        let value = vm.expr_cache.get(&value).unwrap();
        fmt = &value.0;
        cols = &value.1;
    }
    let lazy = vm.source.dataframe_mut_ref().unwrap().lazy.clone();
    let lazy = lazy.with_column(format_str(&fmt, &cols)?.alias("fmt"));
    vm.source.dataframe_mut_ref().unwrap().update(lazy);
    let lazy = vm.source.dataframe_mut_ref().unwrap().lazy.clone();
    let df = lazy.collect()?;
    let fmt = df.column("fmt").unwrap();
    let writer = vm.writer.as_mut().unwrap();
    let writer = writer.as_mut();
    let mut consmer_rows = vm.status.cosumer_rows;
    if vm.status.cosumer_rows >= vm.status.limit {
        return Ok(value::Value::None);
    }
    for i in 0..fmt.len() {
        let value = fmt.get(i)?;
        let s = value.get_str();
        if s.is_none() {
            continue;
        }
        let s = s.unwrap();
        writeln!(writer, "{}", s)?;
        consmer_rows += 1;
        if consmer_rows >= vm.status.limit {
            vm.status.stop = true;
            break;
        }
    }
    vm.status.printed = true;
    Ok(value::Value::None)
}
