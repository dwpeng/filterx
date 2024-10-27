use polars::prelude::{col, lit};

use super::*;

pub fn slice<'a>(
    vm: &'a mut Vm,
    args: &Vec<ast::Expr>,
    inplace: bool,
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

    let col_name = col_name.column()?.col_name;
    let mut length = u32::MAX;

    let start = eval!(vm, &args[1], "Only support start", Constant).u32()?;
    if args.len() == 3 {
        let _length = eval!(vm, &args[2], "Only support length", Constant);
        length = _length.u32()?;
    }

    let e = col(&col_name).slice(lit(start), lit(length));

    if inplace {
        vm.source
            .dataframe_mut_ref()
            .unwrap()
            .with_column(e.alias(&col_name));
        return Ok(value::Value::None);
    }

    Ok(value::Value::Expr(e))
}
