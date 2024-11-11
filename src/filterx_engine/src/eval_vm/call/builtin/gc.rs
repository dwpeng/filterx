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
    let s = s.apply_unary_elementwise(|s| {
        let v = s
            .iter()
            .map(|seq| {
                let seq = seq.get_str().expect(&format!(
                    "Cannot compute GC content of this sequence: {}",
                    s.name()
                ));
                let length = seq.len();
                if length == 0 {
                    return 0.0;
                }
                let gc = seq
                    .bytes()
                    .filter(|c| *c == b'G' || *c == b'C' || *c == b'c' || *c == b'g')
                    .count();
                if gc == 0 {
                    return 0.0;
                }
                (gc as f32) / (length as f32)
            })
            .collect::<Vec<_>>();
        Series::new("gc".into(), v)
    });
    Ok(Some(s))
}

pub fn gc<'a>(vm: &'a mut Vm, args: &Vec<ast::Expr>) -> FilterxResult<value::Value> {
    expect_args_len(args, 1)?;
    let col_name = eval_col!(vm, &args[0], "gc: expected a column name as first argument");
    let col_name = col_name.column()?;
    vm.source.has_column(col_name);
    let e = col(col_name).map(compute_gc, GetOutput::float_type());
    return Ok(value::Value::Expr(e));
}
