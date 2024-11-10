use super::*;
use polars::prelude::{col, Literal};
use value::Value;

pub fn fill<'a>(
    vm: &'a mut Vm,
    args: &Vec<ast::Expr>,
    inplace: bool,
) -> FilterxResult<value::Value> {
    expect_args_len(args, 2)?;

    let col_name = eval_col!(
        vm,
        &args[0],
        "fill: expected a column name as first argument"
    );
    let const_value = eval!(
        vm,
        &args[1],
        "fill: expected a constant value as second argument",
        Constant
    );
    let col_name = col_name.column()?;
    vm.source.has_column(col_name);
    let e = col(col_name).fill_null(const_value.lit());
    if inplace {
        let lazy = &mut vm.source;
        lazy.with_column(e.alias(col_name), None);
        return Ok(Value::None);
    }
    Ok(Value::Expr(e))
}
