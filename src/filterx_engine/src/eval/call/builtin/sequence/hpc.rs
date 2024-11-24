use std::borrow::Cow;

use super::super::*;

use polars::prelude::*;

fn compute_hpc(s: Column) -> PolarsResult<Option<Column>> {
    let ca = s.str()?;
    let ca = ca.apply_values(|s| {
        let mut prev = '*';
        let mut hpc_s = String::with_capacity(s.len());
        for c in s.chars() {
            if c != prev {
                hpc_s.push(c);
            }
            prev = c;
        }
        Cow::Owned(hpc_s)
    });
    Ok(Some(ca.into_column()))
}

pub fn hpc<'a>(
    vm: &'a mut Vm,
    args: &Vec<ast::Expr>,
    inplace: bool,
) -> FilterxResult<value::Value> {
    expect_args_len(args, 1)?;

    let col_name = eval_col!(
        vm,
        &args[0],
        "hpc: expected a column name as first argument"
    );
    let name = col_name.column()?;
    let e = col_name.expr()?;
    let e = e.map(compute_hpc, GetOutput::same_type());
    vm.source_mut().has_column(name);
    if inplace {
        vm.source_mut().with_column(e.clone().alias(name), None);
        return Ok(value::Value::None);
    }
    return Ok(value::Value::named_expr(Some(name.to_string()), e));
}
