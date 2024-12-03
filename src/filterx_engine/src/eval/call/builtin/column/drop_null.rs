use super::super::*;

pub fn drop_null<'a>(vm: &'a mut Vm, args: &Vec<ast::Expr>) -> FilterxResult<value::Value> {
    let mut cols = None;
    if args.len() > 0 {
        let mut some_cols = Vec::new();
        for col_name in args {
            let col_name = eval_col!(vm, col_name, "sss");
            let name = col_name.column()?;
            vm.source_mut().has_column(name);
            let e = col_name.expr()?;
            some_cols.push(e);
        }
        cols = Some(some_cols);
    }

    let lazy = vm.source_mut().lazy();
    let lazy = lazy.drop_nulls(cols);
    vm.source_mut().update(lazy);
    return Ok(value::Value::None);
}
