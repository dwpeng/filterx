use super::*;

pub fn tail(vm: &mut Vm, args: &Vec<ast::Expr>) -> FilterxResult<value::Value> {
    expect_args_len(args, 1)?;
    let n = match args.first().unwrap() {
        ast::Expr::Constant(n) => n.eval(vm)?,
        _ => {
            return Err(FilterxError::RuntimeError(
                "Only support integer".to_string(),
            ));
        }
    };

    let nrow = match n {
        value::Value::Int(i) => {
            if i >= 0 {
                i as usize
            } else {
                return Err(FilterxError::RuntimeError(
                    "Index starts from 1".to_string(),
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
            let lazy = lazy.tail(nrow as u32);
            df_source.lazy = lazy;
        }
        _ => {
            return Err(FilterxError::RuntimeError(
                "Source `{:?}` is not supported.".to_string(),
            ));
        }
    };

    Ok(value::Value::None)
}