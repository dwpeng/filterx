use polars::prelude::col;

use super::*;

pub fn drop_null<'a>(
    vm: &'a mut Vm,
    args: &Vec<ast::Expr>,
    inplace: bool,
) -> FilterxResult<value::Value> {
    expect_args_len(args, 1)?;
    let col_name = eval_col!(
        vm,
        &args[0],
        "len: expected a column name as first argument"
    );
    let col_name = col_name.column()?;
    vm.source.has_column(col_name);
    let e = col(col_name).drop_nulls();
    if inplace {
        vm.source.with_column(e.alias(col_name), None);
        return Ok(value::Value::None);
    }
    Ok(value::Value::Expr(e))
}
