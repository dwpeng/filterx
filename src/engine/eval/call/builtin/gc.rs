use super::*;
use crate::eval;
use crate::FilterxError;
use polars::prelude::*;

use polars::prelude::col;

fn compute_gc(s: Series) -> PolarsResult<Option<Series>> {
    if !s.dtype().is_string() {
        return Err(PolarsError::InvalidOperation(
            format!(
                "Computing GC content needs a string column, got column `{}` with type `{}`",
                s.name(),
                s.dtype()
            )
            .into(),
        ));
    }
    let v = s
        .iter()
        .map(|seq| {
            let seq = seq.get_str().unwrap();
            let mut gc_base = 0;
            for base in seq.chars() {
                match base {
                    'G' | 'C' | 'g' | 'c' => gc_base += 1,
                    _ => {}
                }
            }
            if gc_base == 0 {
                return 0.0;
            }
            return gc_base as f32 / seq.len() as f32;
        })
        .collect::<Vec<f32>>();
    Ok(Some(Series::new("gc".into(), v)))
}

pub fn gc<'a>(vm: &'a mut Vm, args: &Vec<ast::Expr>) -> FilterxResult<value::Value> {
    expect_args_len(args, 1)?;
    let col_name = eval!(
        vm,
        &args[0],
        "gc: expected a Series as first argument",
        Name,
        Call
    );
    let col_name = match col_name {
        value::Value::Column(c) => c.col_name.to_string(),
        _ => {
            return Err(FilterxError::RuntimeError(
                "gc: need a column name".to_string(),
            ))
        }
    };
    let e = col(col_name).map(compute_gc, GetOutput::float_type());
    return Ok(value::Value::Expr(e));
}
