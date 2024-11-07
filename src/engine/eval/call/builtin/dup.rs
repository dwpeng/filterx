use crate::util::check_repeat;

use super::*;
use polars::frame::UniqueKeepStrategy;

pub fn dup<'a>(
    vm: &'a mut Vm,
    args: &Vec<ast::Expr>,
    unique_strategy: UniqueKeepStrategy,
) -> FilterxResult<value::Value> {
    let mut select_dolumns = vec![];
    for arg in args {
        let pass = check_types!(arg, Name, Call);
        if !pass {
            let h = &mut vm.hint;
            h.white("dup only support column name").print_and_exit();
        }
    }
    for arg in args {
        let col = eval!(vm, arg, Name, Call);
        let col = match col {
            value::Value::Name(c) => c.name,
            value::Value::Column(c) => c.col_name,
            _ => {
                let h = &mut vm.hint;
                h.white("dup only support column name").print_and_exit();
            }
        };

        select_dolumns.push(col.clone());
    }

    if check_repeat(&select_dolumns) {
        let h = &mut vm.hint;
        h.white("dup: column name should not repeat, but got: ")
            .cyan(&select_dolumns.join(", "))
            .print_and_exit();
    }

    match &mut vm.source {
        Source::Dataframe(ref mut df_source) => {
            let lazy = df_source.lazy.clone();
            let lazy = lazy.unique_generic(Some(select_dolumns), unique_strategy);
            df_source.update(lazy);
        }
    }

    Ok(value::Value::None)
}
