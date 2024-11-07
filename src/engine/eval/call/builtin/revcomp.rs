use std::borrow::Cow;

use super::*;
use crate::eval;

use polars::error::PolarsResult;
use polars::prelude::col;
use polars::prelude::ChunkApply;
use polars::prelude::GetOutput;
use polars::prelude::*;

fn compute_revcomp(s: Column) -> PolarsResult<Option<Column>> {
    let ca = s.str()?;
    let ca = ca.apply_values(|s| {
        let s = s.chars().rev().collect::<String>();
        let s = s
            .chars()
            .map(|c| match c {
                'A' => 'T',
                'T' => 'A',
                'C' => 'G',
                'G' => 'C',
                'a' => 't',
                't' => 'a',
                'c' => 'g',
                'g' => 'c',
                _ => c,
            })
            .collect::<String>();
        Cow::Owned(s)
    });
    Ok(Some(ca.into_column()))
}

pub fn revcomp<'a>(
    vm: &'a mut Vm,
    args: &Vec<ast::Expr>,
    inplace: bool,
) -> FilterxResult<value::Value> {
    expect_args_len(args, 1)?;

    let pass = check_types!(&args[0], Name, Call);
    if !pass {
        let h = &mut vm.hint;
        h.white("revcomp: expected a column name as first argument")
            .print_and_exit();
    }

    let col_name = eval!(vm, &args[0], Name, Call);
    let col_name = match col_name {
        value::Value::Item(c) => c.col_name,
        value::Value::Name(n) => n.name,
        _ => {
            let h = &mut vm.hint;
            h.white("revcomp: expected a column name as first argument")
                .print_and_exit();
        }
    };
    let e = col(&col_name).map(compute_revcomp, GetOutput::same_type());
    if inplace {
        let lazy = vm.source.lazy();
        let lazy = lazy.with_column(e.clone().alias(&col_name));
        vm.source.update(lazy);
        return Ok(value::Value::None);
    }
    return Ok(value::Value::Expr(e));
}
