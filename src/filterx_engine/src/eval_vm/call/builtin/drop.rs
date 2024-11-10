use super::*;

pub fn drop<'a>(vm: &'a mut Vm, args: &Vec<ast::Expr>) -> FilterxResult<value::Value> {
    let mut drop_columns = vec![];

    for arg in args {
        let col = eval_col!(vm, arg, "drop: expected a column name as argument");
        let col = col.column()?;
        vm.source.has_column(col);
        drop_columns.push(col.to_string());
    }

    vm.source.drop(drop_columns);
    Ok(value::Value::None)
}
