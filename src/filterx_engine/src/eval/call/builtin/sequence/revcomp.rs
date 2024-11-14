use std::borrow::Cow;

use super::super::*;

use polars::prelude::*;

fn compute_revcomp(s: Column) -> PolarsResult<Option<Column>> {
    let ca = s.str()?;
    let ca = ca.apply_values(|s| {
        let s: String = s
            .chars()
            .rev()
            .map(|b| match b {
                'A' => 'T',
                'T' => 'A',
                'C' => 'G',
                'G' => 'C',
                'a' => 't',
                't' => 'a',
                'c' => 'g',
                'g' => 'c',
                _ => b,
            })
            .collect();
        Cow::Owned(s)
    });
    Ok(Some(ca.into_column()))
}

pub fn revcomp<'a>(
    vm: &'a mut Vm,
    args: &Vec<ast::Expr>,
    inplace: bool,
) -> FilterxResult<value::Value> {
    expect_args_len(args, 1)?;

    let col_name = eval_col!(
        vm,
        &args[0],
        "revcomp: expected a column name as first argument"
    );
    let name = col_name.column()?;
    let e = col_name.expr()?;
    vm.source_mut().has_column(name);
    let e = e.map(compute_revcomp, GetOutput::same_type());
    if inplace {
        vm.source_mut().with_column(e.clone().alias(name), None);
        return Ok(value::Value::None);
    }
    return Ok(value::Value::named_expr(Some(name.to_string()), e));
}
