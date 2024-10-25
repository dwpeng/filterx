use polars::prelude::format_str;

use super::*;
use polars::prelude::{col, Expr};

use regex::Regex;
use std::io::Write;

fn parse_format_string(s: &str) -> FilterxResult<(String, Option<Vec<Expr>>)> {
    // value: "xxxxx"  ->  "xxxxx"
    // value: "xxx_{seq}" ->  "xxx_{}" and col("seq")
    // value: "xxx_{seq}_{seq}" -> "xxx_{}_{}" and col("seq"), col("seq")

    if s.is_empty() {
        return Err(FilterxError::RuntimeError(
            "Empty format string".to_string(),
        ));
    }

    if !s.contains("{") {
        return Ok((s.to_string(), None));
    }

    let re = Regex::new(r"\{(\w+)\}").unwrap();
    let fmt = re.replace_all(s, "{}").to_string();
    let cols = re
        .captures_iter(s)
        .map(|x| col(x.get(1).unwrap().as_str()))
        .collect::<Vec<Expr>>();

    Ok((fmt, Some(cols)))
}

#[test]
fn test_parse_format_string() {
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
}

pub fn print<'a>(vm: &'a mut Vm, args: &Vec<ast::Expr>) -> FilterxResult<value::Value> {
    expect_args_len(args, 1)?;

    let value = eval!(
        vm,
        &args[0],
        "Only support column index",
        Constant,
        Name,
        Call
    );
    let value = value.text()?;
    let (fmt, cols) = parse_format_string(&value)?;
    let lazy = vm.source.dataframe_mut_ref().unwrap().lazy.clone();
    let cols = cols.unwrap_or(vec![]);
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
