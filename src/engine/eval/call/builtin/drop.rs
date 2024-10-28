use super::*;

pub fn drop<'a>(vm: &'a mut Vm, args: &Vec<ast::Expr>) -> FilterxResult<value::Value> {
    let mut drop_columns = vec![];
    for arg in args {
        let col = eval!(vm, arg, "Only support column name", Name, Call, UnaryOp);
        let col = match col {
            value::Value::Column(c) => c,
            _ => {
                return Err(FilterxError::RuntimeError(
                    "Only support column name".to_string(),
                ));
            }
        };
        drop_columns.push(col.col_name.clone());
        vm.status.drop_column(&col.col_name);
    }

    match &mut vm.source {
        Source::Dataframe(ref mut df_source) => {
            let lazy = df_source.lazy.clone();
            let lazy = lazy.drop(drop_columns);
            df_source.lazy = lazy;
        }
    }

    Ok(value::Value::None)
}
