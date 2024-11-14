use super::super::*;

pub fn is_na<'a>(vm: &'a mut Vm, args: &Vec<ast::Expr>, not: bool) -> FilterxResult<value::Value> {
    expect_args_len(args, 1)?;
    let col_name = eval_col!(
        vm,
        &args[0],
        "is_na: expected a column name as first argument"
    );
    vm.source_mut().has_column(col_name.column()?);
    let col_expr = col_name.expr()?;
    if not {
        vm.source_mut().filter(col_expr.is_not_nan());
    } else {
        vm.source_mut().filter(col_expr.is_nan());
    }
    Ok(value::Value::None)
}
