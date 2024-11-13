use super::super::*;

pub fn alias<'a>(vm: &'a mut Vm, args: &Vec<ast::Expr>) -> FilterxResult<value::Value> {
    expect_args_len(args, 1)?;
    let col = eval_col!(
        vm,
        &args[0],
        "alias: expected a column name as first argument"
    );
    // auto check column name
    col.column()?;
    Ok(col)
}
