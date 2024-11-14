use std::ops::Deref;

use super::super::ast;
use filterx_core::value;
use filterx_core::value::Value;
use filterx_core::FilterxResult;

use crate::eval::Eval;
use crate::vm::Vm;

use crate::{eval, execuable};

impl<'a> Eval<'a> for ast::StmtAssign {
    type Output = Value;
    fn eval(&self, vm: &'a mut Vm) -> FilterxResult<Self::Output> {
        execuable!(vm, "=");
        if self.targets.len() != 1 {
            let h = &mut vm.hint;
            h.white("Dosn't support unpacking multiple assignment expression")
                .next_line()
                .next_line()
                .green(" Right: alias(new_col) = col1 + col2")
                .next_line()
                .red(" Wrong: alias(new_col) = col1, col2")
                .next_line()
                .print_and_exit();
        }

        let target = self.targets.first().unwrap();

        let pass = match target {
            ast::Expr::Call(c) => {
                let function_name = match c.func.as_ref() {
                    ast::Expr::Name(n) => n.eval(vm)?,
                    _ => unreachable!(),
                };
                match function_name {
                    value::Value::Name(n) => match n.name.as_str() {
                        "alias" => true,
                        _ => false,
                    },
                    _ => false,
                }
            }
            _ => false,
        };
        if !pass {
            let h = &mut vm.hint;
            h.white("Use")
                .cyan(" `alias` ")
                .bold()
                .white("to create a new column.")
                .green(" alias(new_col) = col1 + col2")
                .print_and_exit();
        }

        let new_col = eval!(vm, target, "", Call);
        let new_col_name = new_col.column().unwrap();

        let right = self.value.deref();

        let value = eval!(
            vm,
            right,
            vm.hint
                .white("While using `alias` to create a new column, valid values are:")
                .next_line()
                .cyan("1. Constant, like 1 or 'a'")
                .next_line()
                .cyan("2. Column Name, like col1")
                .next_line()
                .cyan("3. Call, like upper(col1)")
                .next_line()
                .cyan("4. UnaryOp, like -col1")
                .next_line()
                .cyan("5. BinOp, like col1 + col2")
                .next_line()
                .next_line(),
            Constant,
            Name,
            Call,
            UnaryOp,
            BinOp
        );
        let exist = vm
            .source_mut()
            .ret_column_names
            .contains(&new_col_name.to_string());
        let if_append = match exist {
            true => None,
            false => Some(new_col_name.into()),
        };
        vm.source_mut()
            .with_column(value.expr()?.alias(new_col_name), if_append);
        Ok(Value::None)
    }
}
