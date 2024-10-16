use super::*;

pub fn print(args: &Vec<ast::Expr>, _vm: &mut Vm) -> FilterxResult<value::Value> {
    expect_args_len(args, 1)?;

    Ok(value::Value::None)
}

