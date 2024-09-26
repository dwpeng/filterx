use super::*;
use polars::prelude::col;
use polars::prelude::Expr;

pub fn select<'a>(vm: &'a mut Vm, args: &Vec<ast::Expr>) -> FilterxResult<value::Value> {
    let mut select_dolumns = vec![];
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

        select_dolumns.push(col.col_name.clone());
    }
    // check if have repeat column
    let mut check = std::collections::HashSet::new();
    for col in &select_dolumns {
        if check.contains(col) {
            return Err(FilterxError::RuntimeError(
                "Select column can't have repeat column".to_string(),
            ));
        }
        check.insert(col);
    }
    let exprs: Vec<Expr> = select_dolumns.iter().map(|c| col(c)).collect();
    let lazy = vm.lazy.clone();
    let lazy = lazy.select(exprs);
    vm.lazy = lazy;

    Ok(value::Value::None)
}
