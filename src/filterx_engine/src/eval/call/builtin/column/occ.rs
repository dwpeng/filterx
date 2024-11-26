use super::super::*;
use polars::chunked_array::ops::SortMultipleOptions;
use polars::prelude::{col as polars_col, JoinArgs};

pub fn occ(vm: &mut Vm, args: &Vec<ast::Expr>, lte: bool) -> FilterxResult<value::Value> {
    if args.len() < 2 {
        return Err(FilterxError::RuntimeError(
            "occ: expected at least 2 arguments".to_string(),
        ));
    }

    let mut cols = Vec::new();

    let last = args.last().unwrap();
    let nargs = args.len();

    for i in 0..nargs - 1 {
        let arg = &args[i];
        let v = eval_col!(vm, arg, "occ: expected column(s) name as argument(s)");
        let col = v.column()?;
        vm.source_mut().has_column(col);
        cols.push(col.to_string());
    }

    let occ_threshold = eval_int!(vm, last, "occ: expected an integer as last argument");
    let occ_threshold = occ_threshold.int()?;

    if occ_threshold < 1 {
        return Err(FilterxError::RuntimeError(
            "occ: expected an integer >= 1 as last argument".to_string(),
        ));
    }

    let lazy = vm.source_mut().lazy();
    let sort_options = SortMultipleOptions::default()
        .with_maintain_order(true)
        .with_nulls_last(true)
        .with_multithreaded(true);

    let by = cols.clone();
    let first_col = cols.first().unwrap();
    let cols = cols.iter().map(|s| polars_col(s)).collect::<Vec<_>>();
    let lazy_group_by = lazy
        .clone()
        .select(cols.clone())
        .sort(by, sort_options)
        .group_by(cols.clone())
        .agg([polars_col(first_col.as_str())
            .len()
            .alias("__filterx_count__")]);
    // use count to filter
    let mut e = polars_col("__filterx_count__");
    if lte {
        e = e.lt_eq(occ_threshold);
    } else {
        e = e.gt_eq(occ_threshold);
    }
    let lazy_group_by = lazy_group_by.filter(e);
    let lazy_group_by = lazy_group_by.drop(["__filterx_count__"]);
    let lazy = lazy.join(lazy_group_by, cols.clone(), cols, JoinArgs::default());
    vm.source_mut().update(lazy);
    Ok(value::Value::None)
}
