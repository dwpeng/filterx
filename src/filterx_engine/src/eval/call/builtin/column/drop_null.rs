use polars::prelude::col;

use super::super::*;

pub fn drop_null<'a>(
    vm: &'a mut Vm,
    args: &Vec<ast::Expr>,
) -> FilterxResult<value::Value> {
    expect_args_len(args, 1)?;
    let col_name = eval_col!(
        vm,
        &args[0],
        "drop_null: expected a column name as first argument"
    );
    let name = col_name.column()?;
    vm.source_mut().has_column(name);
    let e = col(name).drop_nulls();
    vm.source_mut().with_column(e.alias(name), None);
    return Ok(value::Value::None);
}
