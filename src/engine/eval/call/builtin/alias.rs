use super::*;

pub fn alias<'a>(vm: &'a mut Vm, args: &Vec<ast::Expr>) -> FilterxResult<value::Value> {
    expect_args_len(args, 1)?;

    let pass = check_types!(&args[0], Name, Call);
    if !pass {
        let h = &mut vm.hint;
        h.white("alias: expected a column name as first argument")
            .print_and_exit();
    }

    let col = eval!(
        vm,
        &args[0],
        Name,
        Call
    );

    Ok(col)
}
