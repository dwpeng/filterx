use super::*;
use polars::prelude::*;

use polars::prelude::col;

fn compute_gc(s: Column) -> PolarsResult<Option<Column>> {
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

    let s = s.as_series().unwrap();

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
    Ok(Some(Column::new("gc".into(), v)))
}

pub fn gc<'a>(vm: &'a mut Vm, args: &Vec<ast::Expr>) -> FilterxResult<value::Value> {
    expect_args_len(args, 1)?;
    let col_name = eval_col!(vm, &args[0], "gc: expected a column name as first argument");
    let col_name = col_name.column()?;
    vm.source.has_column(col_name);
    let e = col(col_name).map(compute_gc, GetOutput::float_type());
    return Ok(value::Value::Expr(e));
}
