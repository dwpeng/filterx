use polars::prelude::{col, lit};

use super::*;

pub fn strip<'a>(
    vm: &'a mut Vm,
    args: &Vec<ast::Expr>,
    inplace: bool,
    right: bool,
    left: bool,
) -> FilterxResult<value::Value> {
    expect_args_len(args, 2)?;

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

    let patt = eval!(vm, &args[1], "Only support pattern", Constant);
    let patt = patt.string()?;
    let patt = lit(patt.as_str());

    let e = match (right, left) {
        (true, true) => col(&col_name).str().strip_chars(patt),
        (true, false) => col(&col_name).str().strip_suffix(patt),
        (false, true) => col(&col_name).str().strip_prefix(patt),
        (false, false) => unreachable!(),
    };

    if inplace {
        vm.source
            .dataframe_mut_ref()
            .unwrap()
            .with_column(e.alias(&col_name));
        return Ok(value::Value::None);
    }

    Ok(value::Value::Expr(e))
}
