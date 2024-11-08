use super::*;

use polars::prelude::Literal;

pub fn nr<'a>(_vm: &'a mut Vm, args: &Vec<ast::Expr>) -> FilterxResult<value::Value> {
    expect_args_len(args, 0)?;
    Ok(value::Value::Expr(1.lit().alias("nr").cum_sum(false)))
}
