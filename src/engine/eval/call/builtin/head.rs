use super::*;

pub fn head(vm: &mut Vm, args: &Vec<ast::Expr>) -> FilterxResult<value::Value> {
    expect_args_len(args, 1)?;
    let n = eval!(vm, &args[0], "Only support integer", Constant, UnaryOp);
    let nrow = match n {
        value::Value::Int(i) => {
            if i >= 0 {
                i as usize
            } else {
                return Err(FilterxError::RuntimeError(
                    "Index starts from 0".to_string(),
                ));
            }
        }
        _ => {
            return Err(FilterxError::RuntimeError(
                "Only support integer".to_string(),
            ));
        }
    };

    match &mut vm.source {
        Source::Dataframe(ref mut df_source) => {
            let lazy = df_source.lazy.clone();
            let lazy = lazy.slice(0, nrow as u32);
            df_source.lazy = lazy;
        }
    };

    vm.status.limit = nrow;
    Ok(value::Value::None)
}
