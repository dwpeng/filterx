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

    let mut col = match col {
        value::Value::Column(c) => c,
        _ => {
            return Err(FilterxError::RuntimeError(
                "Only support column name".to_string(),
            ));
        }
    };

    if vm.status.is_column_exist(&col.col_name) {
        col.new = false;
    } else {
        col.new = true;
    }
    col.force = false;
    Ok(value::Value::Column(col))
}
