use std::ops::Deref;

use super::super::ast;
use super::super::value::Value;

use crate::check_types;
use crate::engine::eval::Eval;
use crate::engine::vm::Vm;
use crate::eval;
use crate::FilterxResult;

impl<'a> Eval<'a> for ast::StmtAssign {
    type Output = Value;
    fn eval(&self, vm: &'a mut Vm) -> FilterxResult<Self::Output> {
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
            ast::Expr::Call(_) => true,
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

        let new_col = eval!(vm, target, Call);

        let new_col = match new_col {
            Value::Item(col) => col.col_name,
            Value::Name(n) => n.name,
            _ => {
                let h = &mut vm.hint;
                h.white("Use")
                    .cyan(" `alias` ")
                    .bold()
                    .white("to create a new column.")
                    .green(" alias(new_col) = col1 + col2")
                    .print_and_exit();
            }
        };

        let right = self.value.deref();

        if !check_types!(right, Constant, Name, Call, UnaryOp, BinOp) {
            let h = &mut vm.hint;
            h.white("While using `alias` to create a new column, valid values are:")
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
                .next_line()
                .print_and_exit();
        }

        let value = eval!(vm, right, Constant, Name, Call, UnaryOp, BinOp);

        vm.source
            .with_column(value.expr()?.alias(new_col.clone()), Some(new_col.clone()));

        Ok(Value::None)
    }
}
