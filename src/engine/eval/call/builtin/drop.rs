use super::*;

pub fn drop<'a>(vm: &'a mut Vm, args: &Vec<ast::Expr>) -> FilterxResult<value::Value> {
    let mut drop_columns = vec![];
    for arg in args {
        let col = match arg {
            ast::Expr::Name(n) => n.eval(vm)?,
            ast::Expr::Call(c) => c.eval(vm)?,
            _ => {
                return Err(FilterxError::RuntimeError(
                    "Only support column name".to_string(),
                ));
            }
        };

        let col = match col {
            value::Value::Column(c) => c,
            _ => {
                return Err(FilterxError::RuntimeError(
                    "Only support column name".to_string(),
                ));
            }
        };
        drop_columns.push(col.col_name.clone());
    }
    let lazy = vm.lazy.clone();
    let lazy = lazy.drop(drop_columns);
    vm.lazy = lazy;

    Ok(value::Value::None)
}
