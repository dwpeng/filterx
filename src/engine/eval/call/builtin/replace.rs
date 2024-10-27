use polars::prelude::{col, lit};

use super::*;

pub fn replace<'a>(
    vm: &'a mut Vm,
    args: &Vec<ast::Expr>,
    inplace: bool,
    many: bool,
) -> FilterxResult<value::Value> {
    expect_args_len(args, 3)?;

    let col_name = eval!(
        vm,
        &args[0],
        "Only support column name",
        Name,
        Call,
        UnaryOp
    );

    let col_name = match col_name {
        value::Value::Column(c) => c.col_name,
        _ => {
            return Err(FilterxError::RuntimeError(
                "Only support column name".to_string(),
            ));
        }
    };

    let patt = eval!(vm, &args[1], "Only support pattern", Constant);
    let repl = eval!(vm, &args[2], "Only support replacement", Constant);
    let patt = patt.string()?;
    let repl = repl.string()?;

    let patt = lit(patt.as_str());
    let repl = lit(repl.as_str());

    if inplace {
        vm.source
            .dataframe_mut_ref()
            .unwrap()
            .with_column(match many {
                true => col(&col_name)
                    .str()
                    .replace_all(patt, repl, true)
                    .alias(&col_name),
                false => col(&col_name)
                    .str()
                    .replace(patt, repl, true)
                    .alias(&col_name),
            });
        return Ok(value::Value::None);
    }

    Ok(value::Value::Expr(match many {
        true => col(&col_name)
            .str()
            .replace_all(patt, repl, true)
            .alias(&col_name),
        false => col(&col_name)
            .str()
            .replace(patt, repl, true)
            .alias(&col_name),
    }))
}
