use super::*;

pub fn len<'a>(vm: &'a mut Vm, args: &Vec<ast::Expr>) -> FilterxResult<value::Value> {
    expect_args_len(args, 1)?;
    let pass = check_types!(&args[0], Name, Call);
    if !pass {
        let h = &mut vm.hint;
        h.white("len: expected a column name as first argument")
            .print_and_exit();
    }
    let col_name = eval!(
        vm,
        &args[0],
        Name,
        Call
    );
    let col_name = col_name.expr()?;
    Ok(value::Value::Expr(col_name.str().len_chars()))
}
