use polars::prelude::lit;

use super::super::*;

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
    let name = col_name.column()?;
    let e = col_name.expr()?;
    vm.source_mut().has_column(name);

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
        vm.source_mut().with_column(
            match many {
                true => e.str().replace_all(patt, repl, false).alias(name),
                false => e.str().replace(patt, repl, false).alias(name),
            },
            None,
        );
        return Ok(value::Value::None);
    }

    Ok(value::Value::named_expr(
        Some(name.to_string()),
        match many {
            true => e.str().replace_all(patt, repl, false).alias(name),
            false => e.str().replace(patt, repl, false).alias(name),
        },
    ))
}
