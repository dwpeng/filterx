use super::super::*;

pub fn limit<'a>(vm: &'a mut Vm, args: &Vec<ast::Expr>) -> FilterxResult<value::Value> {
    expect_args_len(args, 1)?;

    let n = eval!(
        vm,
        &args[0],
        "limit: expected a non-negative number as first argument",
        Constant,
        UnaryOp,
        BinOp
    );
    let nrow = match n {
        value::Value::Int(i) => {
            if i >= 0 {
                i as usize
            } else {
                let h = &mut vm.hint;
                h.white("limit: expected a non-negative number as first argument, but got ")
                    .cyan(&format!("{}", i))
                    .print_and_exit();
            }
        }
        _ => {
            let h = &mut vm.hint;
            h.white("limit: expected a non-negative number as first argument")
                .print_and_exit();
        }
    };

    match vm.source_type() {
        SourceType::Fasta | SourceType::Fastq => {
            vm.status.limit_rows = nrow;
            return Ok(value::Value::None);
        }
        _ => {}
    }

    vm.source_mut().slice(0, nrow);

    Ok(value::Value::None)
}
