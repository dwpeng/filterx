use crate::util::check_repeat;

use super::*;
use polars::prelude::col;
use polars::prelude::Expr;

pub fn select<'a>(vm: &'a mut Vm, args: &Vec<ast::Expr>) -> FilterxResult<value::Value> {
    let mut select_dolumns = vec![];

    for arg in args {
        let pass = check_types!(arg, Name, Call);
        if !pass {
            let h = &mut vm.hint;
            h.white("select: expected a column name as first argument")
                .print_and_exit();
        }
    }

    for arg in args {
        let col = eval!(vm, arg, Name, Call);
        let col = match col {
            value::Value::Column(c) => c.col_name,
            value::Value::Name(c) => c.name,
            _ => {
                let h = &mut vm.hint;
                h.white("select: expected a column name as first argument")
                    .print_and_exit();
            }
        };

        select_dolumns.push(col);
    }

    if check_repeat(&select_dolumns) {
        let h = &mut vm.hint;
        h.white("select: don't support duplicate column, but got duplicate column: ")
            .cyan(&select_dolumns.join(", "))
            .print_and_exit();
    }

    match &mut vm.source {
        Source::Dataframe(ref mut df_source) => {
            let exprs: Vec<Expr> = select_dolumns.iter().map(|c| col(c)).collect();
            let lazy = df_source.lazy.clone();
            let lazy = lazy.select(exprs);
            df_source.update(lazy);
        }
    }

    vm.status.select(select_dolumns);

    Ok(value::Value::None)
}
