use super::*;

pub fn col(vm: &mut Vm, args: &Vec<ast::Expr>) -> FilterxResult<value::Value> {
    expect_args_len(args, 1)?;
    let col_index = match args.first().unwrap() {
        ast::Expr::Constant(n) => n.eval(vm)?,
        ast::Expr::Name(n) => n.eval(vm)?,
        ast::Expr::Call(c) => {
            c.eval(vm)?
        }
        _ => {
            return Err(FilterxError::RuntimeError(
                "Only support column index".to_string(),
            ));
        }
    };

    let c = match col_index {
        value::Value::Int(i) => format!("column_{}", i),
        value::Value::Str(s) => s,
        value::Value::Column(c) => c.col_name,
        _ => {
            return Err(FilterxError::RuntimeError(
                "Only support column index".to_string(),
            ));
        }
    };

    Ok(value::Value::Column(value::Column {
        col_name: c,
        force: false,
        new: false,
    }))
}
