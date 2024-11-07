use polars::prelude::{col, lit};

use super::*;

pub fn replace<'a>(
    vm: &'a mut Vm,
    args: &Vec<ast::Expr>,
    inplace: bool,
    many: bool,
) -> FilterxResult<value::Value> {
    expect_args_len(args, 3)?;

    let pass = check_types!(&args[0], Name, Call);
    if !pass {
        let h = &mut vm.hint;
        h.white("replace: expected a column name as first argument")
            .print_and_exit();
    }

    let col_name = eval!(vm, &args[0], Name, Call);
    let col_name = match col_name {
        value::Value::Item(c) => c.col_name,
        value::Value::Name(n) => n.name,
        _ => {
            let h = &mut vm.hint;
            h.white("replace: expected a column name as first argument")
                .print_and_exit();
        }
    };

    let pass = check_types!(&args[1], Constant) && check_types!(&args[2], Constant);
    if !pass {
        let h = &mut vm.hint;
        h.white(
            "replace: expected a constant pattern and replacement as second and third argument",
        )
        .print_and_exit();
    }

    let patt = eval!(vm, &args[1], Constant);
    let repl = eval!(vm, &args[2], Constant);
    let patt = patt.string()?;
    let repl = repl.string()?;

    let patt = lit(patt.as_str());
    let repl = lit(repl.as_str());

    if inplace {
        vm.source.with_column(match many {
            true => col(&col_name)
                .str()
                .replace_all(patt, repl, true)
                .alias(&col_name),
            false => col(&col_name)
                .str()
                .replace(patt, repl, true)
                .alias(&col_name),
        }, None);
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
