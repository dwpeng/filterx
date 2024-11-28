use polars::prelude::format_str;

use crate::vm::VmMode;

use super::super::*;
use polars::prelude::{col, Expr};
use regex::Regex;

use lazy_static::lazy_static;

use filterx_core::util;

lazy_static! {
    static ref REGEX_PATTERN: Regex = Regex::new(r"\{([\(\)a-zA-Z0-9_\-+/*,=\\ ]*)\}").unwrap();
    static ref REGEX_VARNAME: Regex = Regex::new(r"^[_a-zA-Z]+[a-zA-Z_0-9]*$").unwrap();
}

fn parse_format_string(s: &str, vm: &mut Vm) -> FilterxResult<(String, Option<Vec<Expr>>)> {
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
    for cap in re.captures_iter(s) {
        let item = cap.get(1).unwrap().as_str();
        if item.is_empty() {
            return Err(FilterxError::RuntimeError(
                "Error format string, empty format value".to_string(),
            ));
        }
        if REGEX_VARNAME.is_match(item) {
            // recheck columns name
            vm.source_mut().has_column(item);
            cols.push(col(item));
            continue;
        }
        let ast = vm.ast(item)?;
        if !ast.is_expression() {
            let h = &mut vm.hint;
            h.white("Only support expression in ")
                .cyan("print")
                .white(", but got ")
                .red(item)
                .print_and_exit();
        }
        let ast = ast.expression().unwrap();
        vm.set_print_expr(item);
        let ast = ast.eval(vm)?;
        let value = ast.expr()?;
        cols.push(value);
    }
    Ok((fmt, Some(cols)))
}

#[test]
fn test_parse_format_string() {
    use polars::prelude::col;
    let mut vm = Vm::mock(SourceType::Fasta);
    let s = "xxx_{seq}";
    let (fmt, cols) = parse_format_string(s, &mut vm).unwrap();
    assert_eq!(fmt, "xxx_{}");
    assert!(cols.is_some());
    let cols = cols.unwrap();
    assert_eq!(cols.len(), 1);
    assert_eq!(cols[0], col("seq"));

    let s = "xxx_{seq}_{seq}";
    let (fmt, cols) = parse_format_string(s, &mut vm).unwrap();
    assert_eq!(fmt, "xxx_{}_{}");
    assert!(cols.is_some());
    let cols = cols.unwrap();
    assert_eq!(cols.len(), 2);
    assert_eq!(cols[0], col("seq"));
    assert_eq!(cols[1], col("seq"));

    let s = "xxx";
    let (fmt, cols) = parse_format_string(s, &mut vm).unwrap();
    assert_eq!(fmt, "xxx");
    assert!(cols.is_none());

    let s = "xxx{len(seq)}";
    let (fmt, cols) = parse_format_string(s, &mut vm).unwrap();
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
    let (fmt, cols) = if let Some(value) = vm.expr_cache.get(&value) {
        (value.0.clone(), value.1.clone())
    } else {
        vm.set_mode(VmMode::Printable);
        let (fmt_, cols_) = parse_format_string(&value, vm)?;
        vm.set_mode(VmMode::Expression);
        let cols_ = cols_.unwrap_or(vec![]);
        vm.expr_cache
            .insert(value.clone(), (fmt_.clone(), cols_.clone()));
        let value = vm.expr_cache.get(&value).unwrap();
        (value.0.clone(), value.1.clone())
    };
    let lazy = vm
        .source_mut()
        .lazy()
        .select([format_str(&fmt, &cols)?.alias(FORMAT_COLUMN_NAME)]);
    let mut df = lazy.collect()?;
    let need = vm.status.limit_rows - vm.status.consume_rows;
    let need = need.min(df.height());
    if need == 0 {
        return Ok(value::Value::None);
    }
    if need < df.height() {
        df = df.slice(0, need);
    }

    util::write_df(&mut df, &mut vm.writer, false, None, None, Some(" "))?;

    vm.status.consume_rows += need;
    vm.status.printed = true;
    Ok(value::Value::None)
}
