use super::super::*;
use polars::chunked_array::ops::SortMultipleOptions;

pub fn sort(vm: &mut Vm, args: &Vec<ast::Expr>, incr: bool) -> FilterxResult<value::Value> {
    let mut cols = Vec::new();

    for arg in args {
        let v = eval_col!(vm, arg, "sort: expected column(s) name as argument(s)");
        let col = v.column()?;
        vm.source_mut().has_column(col);
        cols.push(col.to_string());
    }

    let lazy = vm.source_mut().lazy();
    let sort_options = SortMultipleOptions::default()
        .with_maintain_order(true)
        .with_order_descending(!incr)
        .with_nulls_last(true)
        .with_multithreaded(true);

    let lazy = lazy.sort(cols, sort_options);
    vm.source_mut().update(lazy);

    Ok(value::Value::None)
}
