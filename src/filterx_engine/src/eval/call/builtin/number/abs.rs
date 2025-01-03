use super::super::*;

pub fn abs<'a>(
    vm: &'a mut Vm,
    args: &Vec<ast::Expr>,
    inplace: bool,
) -> FilterxResult<value::Value> {
    expect_args_len(args, 1)?;
    let col_name = eval_col!(
        vm,
        &args[0],
        "abs: expected a column name as first argument"
    );
    let name = col_name.column()?;
    let e = col_name.expr()?;
    vm.source_mut().has_column(name);
    if inplace {
        vm.source_mut().with_column(e.alias(name), None);
        return Ok(value::Value::None);
    }
    Ok(value::Value::named_expr(None, e))
}
