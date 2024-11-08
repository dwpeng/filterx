use super::*;

pub fn abs<'a>(
    vm: &'a mut Vm,
    args: &Vec<ast::Expr>,
    inplace: bool,
) -> FilterxResult<value::Value> {
    expect_args_len(args, 1)?;
    let pass = check_types!(&args[0], Name, Call);
    if !pass {
        let h = &mut vm.hint;
        h.white("abs: expected a column name as first argument")
            .print_and_exit();
    }
    let col_name = eval!(vm, &args[0], Name, Call);
    let e = col_name.expr()?.abs();
    if inplace {
        vm.source.with_column(e, None);
        return Ok(value::Value::None);
    }
    Ok(value::Value::Expr(e))
}
