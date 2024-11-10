use polars::prelude::{col, lit};

use super::*;

pub fn replace<'a>(
    vm: &'a mut Vm,
    args: &Vec<ast::Expr>,
    inplace: bool,
    many: bool,
) -> FilterxResult<value::Value> {
    expect_args_len(args, 3)?;

    let col_name = eval_col!(
        vm,
        &args[0],
        "replace: expected a column name as first argument"
    );
    let col_name = col_name.column()?;
    vm.source.has_column(col_name);

    let patt = eval!(
        vm,
        &args[1],
        "replace: expected a constant pattern and replacement as second argument",
        Constant
    );
    let repl = eval!(
        vm,
        &args[2],
        "replace: expected a constant pattern and replacement as third argument",
        Constant
    );
    let patt = patt.string()?;
    let repl = repl.string()?;

    let patt = lit(patt.as_str());
    let repl = lit(repl.as_str());

    if inplace {
        vm.source.with_column(
            match many {
                true => col(col_name)
                    .str()
                    .replace_all(patt, repl, true)
                    .alias(col_name),
                false => col(col_name)
                    .str()
                    .replace(patt, repl, true)
                    .alias(col_name),
            },
            None,
        );
        return Ok(value::Value::None);
    }

    Ok(value::Value::Expr(match many {
        true => col(col_name)
            .str()
            .replace_all(patt, repl, true)
            .alias(col_name),
        false => col(col_name)
            .str()
            .replace(patt, repl, true)
            .alias(col_name),
    }))
}
