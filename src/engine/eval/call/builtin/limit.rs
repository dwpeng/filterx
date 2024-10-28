use crate::engine::vm::VmSourceType;

use super::*;

pub fn limit<'a>(vm: &'a mut Vm, args: &Vec<ast::Expr>) -> FilterxResult<value::Value> {
    expect_args_len(args, 1)?;

    let n = eval!(vm, &args[0], "Only support integer", Constant, UnaryOp, BinOp);
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

    match vm.source_type{
        VmSourceType::Fasta | VmSourceType::Fastq => {
            vm.status.limit = nrow;
            return Ok(value::Value::None);
        }
        _ => {}
    }

    match &mut vm.source {
        Source::Dataframe(ref mut df_source) => {
            let lazy = df_source.lazy.clone();
            let lazy = lazy.slice(0, nrow as u32);
            df_source.lazy = lazy;
        }
    };

    Ok(value::Value::None)
}
