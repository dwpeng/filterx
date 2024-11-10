use std::borrow::Cow;

use super::*;

use polars::prelude::col;
use polars::prelude::*;

fn compute_revcomp(s: Column) -> PolarsResult<Option<Column>> {
    let ca = s.str()?;
    let ca = ca.apply_values(|s| {
        let s = s.chars().rev().collect::<String>();
        let s = s
            .chars()
            .map(|c| match c {
                'A' => 'T',
                'T' => 'A',
                'C' => 'G',
                'G' => 'C',
                'a' => 't',
                't' => 'a',
                'c' => 'g',
                'g' => 'c',
                _ => c,
            })
            .collect::<String>();
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
    let col_name = col_name.column()?;
    vm.source.has_column(col_name);
    let e = col(col_name).map(compute_revcomp, GetOutput::same_type());
    if inplace {
        vm.source.with_column(e.clone().alias(col_name), None);
        return Ok(value::Value::None);
    }
    return Ok(value::Value::Expr(e));
}
