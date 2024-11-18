use super::super::*;
use filterx_core::value::Value;
use polars::prelude::Literal;

pub fn fill<'a>(
    vm: &'a mut Vm,
    args: &Vec<ast::Expr>,
    inplace: bool,
    is_na: bool,
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
    let name = col_name.column()?;
    let mut e = col_name.expr()?;
    vm.source_mut().has_column(name);
    if is_na{
        e = e.fill_nan(const_value.lit());
    }else{
        e = e.fill_null(const_value.lit());
    }
    if inplace {
        let lazy = vm.source_mut();
        lazy.with_column(e.alias(name), None);
        return Ok(Value::None);
    }
    Ok(Value::named_expr(Some(name.to_string()), e))
}
