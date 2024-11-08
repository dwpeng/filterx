use super::*;

pub fn is_null<'a>(
    vm: &'a mut Vm,
    args: &Vec<ast::Expr>,
    not: bool,
) -> FilterxResult<value::Value> {
    expect_args_len(args, 1)?;
    let pass = check_types!(&args[0], Name, Call);
    if !pass {
        let h = &mut vm.hint;
        h.white("is_null: expected a column name as first argument")
            .print_and_exit();
    }
    let col_name = eval!(vm, &args[0], Name, Call);
    let col_expr = col_name.expr()?;
    if not {
        vm.source.filter(col_expr.is_not_null());
    } else {
        vm.source.filter(col_expr.is_null());
    }
    Ok(value::Value::None)
}