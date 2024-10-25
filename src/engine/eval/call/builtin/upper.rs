use polars::prelude::col;

use super::*;

pub fn upper<'a>(
    vm: &'a mut Vm,
    args: &Vec<ast::Expr>,
    inplace: bool,
) -> FilterxResult<value::Value> {
    expect_args_len(args, 1)?;

    let col_name = eval!(
        vm,
        &args[0],
        "Only support column name",
        Name,
        Call,
        UnaryOp
    );

    let col_name = match col_name {
        value::Value::Column(c) => c.col_name,
        _ => {
            return Err(FilterxError::RuntimeError(
                "Only support column name".to_string(),
            ));
        }
    };

    if inplace {
        vm.source
            .dataframe_mut_ref()
            .unwrap()
            .with_column(col(&col_name).str().to_uppercase().alias(&col_name));
        return Ok(value::Value::None);
    }

    Ok(value::Value::Expr(col(col_name).str().to_uppercase()))
}
