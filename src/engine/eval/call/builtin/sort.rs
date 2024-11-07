use super::*;
use polars::chunked_array::ops::SortMultipleOptions;

pub fn sort(vm: &mut Vm, args: &Vec<ast::Expr>, incr: bool) -> FilterxResult<value::Value> {
    let mut cols = Vec::new();

    for arg in args {
        let pass = check_types!(arg, Name, Call);
        if !pass {
            let h = &mut vm.hint;
            h.white("sort: expected column(s) name as argument(s)")
                .print_and_exit();
        }
    }

    for arg in args {
        let v = eval!(vm, arg, Name, Call);
        let col = match v {
            value::Value::Int(i) => {
                if i >= 0 {
                    DataframeSource::index2column(i as usize)
                } else {
                    let h = &mut vm.hint;
                    h.white(
                        "while using number index, column index should be positive integer and start from 1."
                    )
                    .print_and_exit();
                }
            }
            value::Value::Str(s) => s,
            value::Value::Item(c) => c.col_name,
            value::Value::Name(c) => c.name,
            _ => {
                let h = &mut vm.hint;
                h.white("sort: expected a column(s) name as argument(s)")
                    .print_and_exit();
            }
        };
        cols.push(col);
    }

    let lazy = vm.source.lazy();
    let sort_options = SortMultipleOptions::default()
        .with_maintain_order(true)
        .with_order_descending(!incr)
        .with_nulls_last(true)
        .with_multithreaded(true);

    let lazy = lazy.sort(cols, sort_options);
    vm.source.update(lazy);

    Ok(value::Value::None)
}
