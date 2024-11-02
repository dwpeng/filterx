use super::*;

pub fn len<'a>(vm: &'a mut Vm, args: &Vec<ast::Expr>) -> FilterxResult<value::Value> {
    expect_args_len(args, 1)?;

    let col_name = eval!(
        vm,
        &args[0],
        "Only support column name",
        Name,
        Call
    );
    let col_name = col_name.expr()?;
    Ok(value::Value::Expr(col_name.str().len_chars()))
}
