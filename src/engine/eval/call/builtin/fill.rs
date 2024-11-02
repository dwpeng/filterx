use super::*;
use polars::prelude::Expr;
use value::Value;

pub fn fill<'a>(
    vm: &'a mut Vm,
    args: &Vec<ast::Expr>,
    inplace: bool,
) -> FilterxResult<value::Value> {
    expect_args_len(args, 2)?;
    let col_name = eval!(vm, &args[0], "Only support column name", Name, Call);
    let col_name = col_name.expr()?;
    let const_value = eval!(vm, &args[1], "Only support constant value", Constant).expr()?;
    match const_value {
        Expr::Literal(_) => {}
        _ => {
            return Err(FilterxError::RuntimeError(format!(
                "Only support constant value, but got: {:?}",
                const_value
            )));
        }
    }
    let e = col_name.fill_null(const_value);

    if inplace {
        let lazy = vm.source.dataframe_mut_ref().unwrap();
        lazy.with_column(e);
        return Ok(Value::None);
    }
    Ok(Value::Expr(e))
}
