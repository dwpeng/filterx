use super::*;

pub fn alias<'a>(vm: &'a mut Vm, args: &Vec<ast::Expr>) -> FilterxResult<value::Value> {
    expect_args_len(args, 1)?;

    let col = eval!(
        vm,
        &args[0],
        "Only support column name",
        Name,
        Call,
        UnaryOp
    );
    
    Ok(col)
}
