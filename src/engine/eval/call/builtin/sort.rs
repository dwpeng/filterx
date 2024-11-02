use super::*;
use polars::chunked_array::ops::SortMultipleOptions;

pub fn sort(vm: &mut Vm, args: &Vec<ast::Expr>, incr: bool) -> FilterxResult<value::Value> {
    let mut cols = Vec::new();
    for arg in args {
        let v = eval!(vm, arg, "Only support column index", Name, Call);
        let col = match v {
            value::Value::Int(i) => {
                if i >= 0 {
                    DataframeSource::index2column(i as usize)
                } else {
                    return Err(FilterxError::RuntimeError(
                        "Index starts from 1".to_string(),
                    ));
                }
            }
            value::Value::Str(s) => s,
            value::Value::Column(c) => c.col_name,
            _ => {
                return Err(FilterxError::RuntimeError(
                    "Only support column index".to_string(),
                ));
            }
        };
        cols.push(col);
    }

    match &mut vm.source {
        Source::Dataframe(ref mut df_source) => {
            let lazy = df_source.lazy.clone();
            let sort_options = SortMultipleOptions::default()
                .with_maintain_order(true)
                .with_order_descending(!incr)
                .with_nulls_last(true)
                .with_multithreaded(true);

            let lazy = lazy.sort(cols, sort_options);
            df_source.lazy = lazy;
        }
    };

    Ok(value::Value::None)
}
