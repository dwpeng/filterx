use crate::util::check_repeat;

use super::super::*;
use polars::frame::UniqueKeepStrategy;

pub fn dup<'a>(
    vm: &'a mut Vm,
    args: &Vec<ast::Expr>,
    unique_strategy: UniqueKeepStrategy,
) -> FilterxResult<value::Value> {
    let mut select_dolumns = vec![];
    for arg in args {
        let col = eval_col!(vm, arg, "dup only support column name");
        let col = col.column()?;
        vm.source_mut().has_column(col);
        select_dolumns.push(col.to_string());
    }

    if check_repeat(&select_dolumns) {
        let h = &mut vm.hint;
        h.white("dup: column name should not repeat, but got: ")
            .cyan(&select_dolumns.join(", "))
            .print_and_exit();
    }

    vm.source_mut().unique(select_dolumns, unique_strategy);

    Ok(value::Value::None)
}
