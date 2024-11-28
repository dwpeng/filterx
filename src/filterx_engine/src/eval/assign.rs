use std::ops::Deref;

use super::super::ast;
use filterx_core::value;
use filterx_core::value::Value;
use filterx_core::FilterxResult;

use crate::eval::Eval;
use crate::vm::Vm;

use crate::{eval, eval_col, execuable};

use polars::prelude::col as polars_col;

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

        let new_col = eval_col!(vm, target, "A column needed in left `=` expression");
        let new_col_name = new_col.column()?;
        let exist = vm.source().check_column(new_col_name);

        if !exist && !pass {
            let h = &mut vm.hint;
            h.white("Use")
                .cyan(" `alias` ")
                .bold()
                .white("to create a new column.")
                .green(" alias(new_col) = col1 + col2")
                .print_and_exit();
        }

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
        let if_append = match exist {
            true => None,
            false => Some(new_col_name.into()),
        };
        vm.source_mut()
            .with_column(value.expr()?.alias(new_col_name), if_append);
        Ok(Value::None)
    }
}

impl<'a> Eval<'a> for ast::StmtAugAssign {
    type Output = Value;
    fn eval(&self, vm: &'a mut Vm) -> FilterxResult<Self::Output> {
        execuable!(vm, &format!("{:?}", self.op));
        let target = eval!(vm, self.target.deref(), "A column needed.", Call, Name);
        let target_name = target.column()?;

        vm.source().has_column(target_name);
        let value = eval!(
            vm,
            self.value.deref(),
            "A const or expression needed.",
            Constant,
            UnaryOp,
            BinOp
        );

        let target = polars_col(target_name);
        let value = value.expr()?;

        let e = match self.op {
            ast::Operator::Add => target + value,
            ast::Operator::Sub => target - value,
            ast::Operator::Mult => target * value,
            ast::Operator::Div => target / value,
            ast::Operator::Mod => target % value,
            ast::Operator::BitAnd => target.and(value),
            ast::Operator::BitOr => target.or(value),
            _ => {
                let h = &mut vm.hint;
                h.white("Only support binary op: ")
                    .cyan("+, -, *, /, %, &, |")
                    .print_and_exit();
            }
        };

        let e = e.alias(target_name);
        vm.source_mut().with_column(e, None);
        Ok(Value::None)
    }
}
