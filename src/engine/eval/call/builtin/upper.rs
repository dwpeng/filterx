use polars::prelude::col;

use super::*;

pub fn upper<'a>(
    vm: &'a mut Vm,
    args: &Vec<ast::Expr>,
    inplace: bool,
) -> FilterxResult<value::Value> {
    expect_args_len(args, 1)?;

    let col_name = eval_col!(
        vm,
        &args[0],
        "upper: expected a column name as first argument"
    );
    let col_name = col_name.column()?;
    vm.source.has_column(col_name);
    if inplace {
        vm.source
            .with_column(col(col_name).str().to_uppercase().alias(col_name), None);
        return Ok(value::Value::None);
    }

    Ok(value::Value::Expr(col(col_name).str().to_uppercase()))
}
