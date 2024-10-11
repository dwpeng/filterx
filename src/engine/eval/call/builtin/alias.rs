use super::*;

pub fn alias<'a>(vm: &'a mut Vm, args: &Vec<ast::Expr>) -> FilterxResult<value::Value> {
    expect_args_len(args, 1)?;

    let col = match args.first().unwrap() {
        ast::Expr::Name(n) => n.eval(vm)?,
        ast::Expr::Call(c) => c.eval(vm)?,
        ast::Expr::UnaryOp(u) => u.eval(vm)?,
        _ => {
            return Err(FilterxError::RuntimeError(
                "Only support column name".to_string(),
            ));
        }
    };

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
