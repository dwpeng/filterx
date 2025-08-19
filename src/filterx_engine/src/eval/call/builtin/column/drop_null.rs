use polars::prelude::by_name;

use super::super::*;

pub fn drop_null<'a>(vm: &'a mut Vm, args: &Vec<ast::Expr>) -> FilterxResult<value::Value> {
    if args.len() == 0 {
        return Ok(value::Value::None);
    }
    let mut some_cols = Vec::new();
    for col_name in args {
        let col_name = eval_col!(vm, col_name, "sss");
        let name = col_name.column()?;
        vm.source_mut().has_column(name);
        some_cols.push(name.to_string());
    }
    let lazy = vm.source_mut().lazy();
    let lazy = lazy.drop_nulls(Some(by_name(some_cols, true)));
    vm.source_mut().update(lazy);
    Ok(value::Value::None)
}
