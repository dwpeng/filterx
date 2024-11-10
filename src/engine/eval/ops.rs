use std::ops::Deref;

use polars::prelude::*;
use rustpython_parser::ast::located::CmpOp;

use super::super::ast;
use crate::engine::value::Value;
use crate::hint::Hint;
use crate::util;

use crate::engine::eval::Eval;
use crate::engine::vm::Vm;
use crate::eval;
use crate::{FilterxError, FilterxResult};

impl<'a> Eval<'a> for ast::ExprUnaryOp {
    type Output = Value;
    fn eval(&self, vm: &'a mut Vm) -> FilterxResult<Self::Output> {
        let v = eval!(
            vm,
            self.operand.deref(),
            vm.hint
                .white("only support constant, call, unaryop, name, BinOp, ")
                .white("example: ")
                .cyan("-1")
                .white(", ")
                .cyan("-(a + 1)")
                .white(", ")
                .cyan("-(a)")
                .white(", ")
                .cyan("-gc(seq)")
                .next_line()
                .white("Got: ")
                .red(&format!("{:?}", self.operand.deref())),
            Constant,
            Call,
            UnaryOp,
            Name
        );

        match self.op {
            ast::UnaryOp::Invert | ast::UnaryOp::Not | ast::UnaryOp::UAdd => {
                let h = &mut vm.hint;
                h.white("only support -")
                    .white("example: ")
                    .cyan("-1")
                    .white(", ")
                    .cyan("-(a + 1)")
                    .white(", ")
                    .cyan("-(a)")
                    .white(", ")
                    .cyan("-gc(seq)")
                    .print_and_exit();
            }
            _ => {}
        }

        match &v {
            Value::Int(_) | Value::Float(_) => {
                let r = unary(v, self.op)?;
                return Ok(r);
            }
            Value::Name(_) | Value::Item(_) => {
                return Ok(Value::Expr(-(v.expr()?)));
            }
            Value::Expr(e) => {
                return Ok(Value::Expr(-e.clone()));
            }
            _ => {
                unreachable!();
            }
        }
    }
}

fn unary(v: Value, _op: ast::UnaryOp) -> FilterxResult<Value> {
    match v {
        Value::Int(i) => Ok(Value::Int(-i)),
        Value::Float(f) => return Ok(Value::Float(-f)),

        _ => unreachable!(),
    }
}

/// pub enum Operator {
///    Add,     *
///    Sub,     *
///    Mult,    *
///    MatMult,
///    Div,     *
///    Mod,
///    Pow,
///    LShift,
///    RShift,
///    BitOr,
///    BitXor,
///    BitAnd,
///    FloorDiv,
///}
impl<'a> Eval<'a> for ast::ExprBinOp {
    type Output = Value;
    fn eval(&self, vm: &'a mut Vm) -> FilterxResult<Self::Output> {
        let l = eval!(
            vm,
            self.left.deref(),
            "Only support constant and constant, column and constant, column and column",
            Constant,
            Call,
            UnaryOp,
            Name,
            BinOp
        );
        let r = eval!(
            vm,
            self.right.deref(),
            "Only support constant and constant, column and constant, column and column",
            Constant,
            Call,
            UnaryOp,
            Name,
            BinOp
        );

        match self.op {
            ast::Operator::Add
            | ast::Operator::Sub
            | ast::Operator::Mult
            | ast::Operator::Div
            | ast::Operator::Mod
            | ast::Operator::BitAnd
            | ast::Operator::BitOr => {}
            _ => {
                let h = &mut vm.hint;
                h.white("Only support binary op: ")
                    .cyan("+, -, *, /, %, &, |")
                    .print_and_exit();
            }
        }

        if l.is_const() && r.is_const() {
            let ret = match self.op {
                ast::Operator::Add => binop(l, r, ast::Operator::Add),
                ast::Operator::Sub => binop(l, r, ast::Operator::Sub),
                ast::Operator::Mult => binop(l, r, ast::Operator::Mult),
                ast::Operator::Div => binop(l, r, ast::Operator::Div),
                ast::Operator::Mod => binop(l, r, ast::Operator::Mod),
                ast::Operator::BitAnd => binop(l, r, ast::Operator::BitAnd),
                ast::Operator::BitOr => binop(l, r, ast::Operator::BitOr),
                _ => {
                    unreachable!();
                }
            };
            return Ok(Value::Expr(ret.expr()?));
        }

        if l.is_expr() || r.is_expr() {
            let l = l.expr()?;
            let r = r.expr()?;
            let ret = match self.op {
                ast::Operator::Add => l + r,
                ast::Operator::Sub => l - r,
                ast::Operator::Mult => l * r,
                ast::Operator::Div => l / r,
                ast::Operator::Mod => l % r,
                ast::Operator::BitAnd => l.and(r),
                ast::Operator::BitOr => l.or(r),
                _ => {
                    unreachable!();
                }
            };
            return Ok(Value::Expr(ret));
        }
        binop_for_dataframe(l, r, self.op)
    }
}

fn binop(l: Value, r: Value, op: ast::Operator) -> Value {
    match op {
        ast::Operator::Add => match (l, r) {
            (Value::Int(l), Value::Int(r)) => Value::Int(l + r),
            (Value::Float(l), Value::Float(r)) => Value::Float(l + r),
            (Value::Str(l), Value::Str(r)) => {
                let mut new = String::with_capacity(l.len() + r.len());
                new.push_str(&l);
                new.push_str(&r);
                Value::Str(new)
            }
            (Value::Float(l), Value::Int(r)) => Value::Float(l + r as f64),
            (Value::Int(l), Value::Float(r)) => Value::Float(l as f64 + r),
            (l, r) => {
                let mut h = Hint::new();
                h.white("can't perform add(+) operation bwtween left: ")
                    .cyan(&format!("{}", l))
                    .white(" and right: ")
                    .cyan(&format!("{}", r))
                    .print_and_exit();
            }
        },
        ast::Operator::Sub => match (l, r) {
            (Value::Int(l), Value::Int(r)) => Value::Int(l - r),
            (Value::Float(l), Value::Float(r)) => Value::Float(l - r),
            (Value::Float(l), Value::Int(r)) => Value::Float(l - r as f64),
            (Value::Int(l), Value::Float(r)) => Value::Float(l as f64 - r),
            (l, r) => {
                let mut h = Hint::new();
                h.white("can't perform sub(-) operation bwtween left: ")
                    .cyan(&format!("{}", l))
                    .white(" and right: ")
                    .cyan(&format!("{}", r))
                    .print_and_exit();
            }
        },
        ast::Operator::Mult => match (l, r) {
            (Value::Int(l), Value::Int(r)) => Value::Int(l * r),
            (Value::Float(l), Value::Float(r)) => Value::Float(l * r),
            (Value::Float(l), Value::Int(r)) => Value::Float(l * r as f64),
            (Value::Int(l), Value::Float(r)) => Value::Float(l as f64 * r),
            (l, r) => {
                let mut h = Hint::new();
                h.white("can't perform mult(*) operation bwtween left: ")
                    .cyan(&format!("{}", l))
                    .white(" and right: ")
                    .cyan(&format!("{}", r))
                    .print_and_exit();
            }
        },
        ast::Operator::Div => match (l, r) {
            (Value::Int(l), Value::Int(r)) => Value::Int(l / r),
            (Value::Float(l), Value::Float(r)) => Value::Float(l / r),
            (Value::Float(l), Value::Int(r)) => Value::Float(l / r as f64),
            (Value::Int(l), Value::Float(r)) => Value::Float(l as f64 / r),
            (l, r) => {
                let mut h = Hint::new();
                h.white("can't perform div(/) operation bwtween left: ")
                    .cyan(&format!("{}", l))
                    .white(" and right: ")
                    .cyan(&format!("{}", r))
                    .print_and_exit();
            }
        },
        ast::Operator::Mod => match (l.clone(), r.clone()) {
            (Value::Int(l), Value::Int(r)) => Value::Int(l % r),
            (l, r) => {
                let mut h = Hint::new();
                h.white("can't perform mod(%) operation bwtween left: ")
                    .cyan(&format!("{}", l))
                    .white(" and right: ")
                    .cyan(&format!("{}", r))
                    .print_and_exit();
            }
        },
        ast::Operator::BitAnd => match (l.clone(), r.clone()) {
            (Value::Int(l), Value::Int(r)) => Value::Int(l & r),
            (l, r) => {
                let mut h = Hint::new();
                h.white("can't perform bitand(&) operation bwtween left: ")
                    .cyan(&format!("{}", l))
                    .white(" and right: ")
                    .cyan(&format!("{}", r))
                    .print_and_exit();
            }
        },
        ast::Operator::BitOr => match (l.clone(), r.clone()) {
            (Value::Int(l), Value::Int(r)) => Value::Int(l | r),
            (l, r) => {
                let mut h = Hint::new();
                h.white("can't perform bitor(|) operation bwtween left: ")
                    .cyan(&format!("{}", l))
                    .white(" and right: ")
                    .cyan(&format!("{}", r))
                    .print_and_exit();
            }
        },
        _ => unreachable!(),
    }
}

fn binop_for_dataframe(left: Value, right: Value, op: ast::Operator) -> FilterxResult<Value> {
    let ret = match op {
        ast::Operator::Add => left.expr()? + right.expr()?,
        ast::Operator::Sub => left.expr()? - right.expr()?,
        ast::Operator::Mult => left.expr()? * right.expr()?,
        ast::Operator::Div => left.expr()? / right.expr()?,
        ast::Operator::Mod => left.expr()? % right.expr()?,
        ast::Operator::BitAnd => left.expr()?.and(right.expr()?),
        ast::Operator::BitOr => left.expr()?.or(right.expr()?),
        _ => unreachable!(),
    };
    Ok(Value::Expr(ret))
}

/// and, or
impl<'a> Eval<'a> for ast::ExprBoolOp {
    type Output = Value;
    fn eval(&self, vm: &'a mut Vm) -> FilterxResult<Self::Output> {
        let left = &self.values[0];
        let vm_apply_lazy = vm.status.apply_lazy;
        vm.status.update_apply_lazy(false);

        let left = eval!(
            vm,
            left,
            "Only support chain compare, like a > 1 and a < 2",
            Compare,
            BoolOp
        );
        let right = &self.values[1];
        let right = eval!(
            vm,
            right,
            "Only support chain compare, like a > 1 and a < 2",
            Compare,
            BoolOp
        );
        vm.status.update_apply_lazy(vm_apply_lazy);

        boolop_in_dataframe(vm, &left, &right, self.op.clone())
    }
}

fn boolop_in_dataframe<'a>(
    vm: &'a mut Vm,
    l: &Value,
    r: &Value,
    op: ast::BoolOp,
) -> FilterxResult<Value> {
    let vm_apply_lazy = vm.status.apply_lazy;
    if !vm_apply_lazy {
        let e = match op {
            ast::BoolOp::And => Ok(Value::Expr(l.expr()?.and(r.clone().expr()?))),
            ast::BoolOp::Or => Ok(Value::Expr(l.expr()?.or(r.clone().expr()?))),
        };
        return e;
    }

    match op {
        ast::BoolOp::And => match (l, r) {
            (_, _) => {
                vm.source.filter(l.expr()?.and(r.clone().expr()?));
            }
        },
        ast::BoolOp::Or => match (l, r) {
            (_, _) => {
                vm.source.filter(l.expr()?.or(r.expr()?));
            }
        },
    }
    Ok(Value::None)
}

/// pub enum CmpOp {
///     Eq,
///     NotEq,
///     Lt,
///     LtE,
///     Gt,
///     GtE,
///     Is,
///     IsNot,
///     In,
///     NotIn,
/// }

/// build lazyframe query plan
impl<'a> Eval<'a> for ast::ExprCompare {
    type Output = Value;
    fn eval(&self, vm: &'a mut Vm) -> FilterxResult<Self::Output> {
        if self.ops.len() > 1 {
            let h = &mut vm.hint;
            h.white("Only support one compare op, like a > 1. If you want to chain compare, use `and` or `or`")
                .print_and_exit();
        }

        let right = &self.comparators[0];

        let left = eval!(
            vm,
            self.left.deref(),
            vm.hint
                .white("In `in` compare, left must be column, or string constant, ")
                .white("example: ")
                .cyan("'a' in a")
                .white(", ")
                .cyan("a in (1, 2, 3)"),
            Constant,
            Call,
            UnaryOp,
            BinOp,
            BoolOp,
            Name
        );
        let op = &self.ops[0];
        let right = eval!(
            vm,
            right,
            vm.hint
                .white("In `in` compare, right must be column, or constant, ")
                .white("example: ")
                .cyan("'a' in a")
                .white(", ")
                .cyan("a in (1, 2, 3)"),
            Constant,
            Call,
            UnaryOp,
            BinOp,
            BoolOp,
            Name,
            Tuple
        );
        compare_in_datarame(vm, left, right, op)
    }
}

fn compare_in_datarame<'a>(
    vm: &'a mut Vm,
    left: Value,
    right: Value,
    op: &CmpOp,
) -> FilterxResult<Value> {
    match op {
        CmpOp::In | CmpOp::NotIn => {
            if left.is_str() && right.is_column() {
                return str_in_col(vm, left, right, op);
            }
            if vm.status.apply_lazy {
                return compare_in_and_not_in_dataframe(vm, left, right, op);
            } else {
                let h = &mut vm.hint;
                h.white("in/not in operator can't be used with and/or")
                    .print_and_exit();
            }
        }
        CmpOp::Eq | CmpOp::NotEq | CmpOp::Lt | CmpOp::LtE | CmpOp::Gt | CmpOp::GtE => {
            return compare_cond_expr_in_dataframe(vm, left, right, op)
        }
        _ => {
            let h = &mut vm.hint;
            h.white("Only support compare op : ==, !=, >, >=, <, <=")
                .print_and_exit();
        }
    };
}

fn str_in_col<'a>(vm: &'a mut Vm, left: Value, right: Value, op: &CmpOp) -> FilterxResult<Value> {
    let left_str = match &left {
        Value::Str(s) => s.clone(),
        _ => unreachable!(),
    };
    let right_col = match &right {
        Value::Item(c) => c.col_name.clone(),
        _ => unreachable!(),
    };

    let e = match op {
        CmpOp::In => col(right_col).str().contains(left_str.lit(), true),
        CmpOp::NotIn => col(right_col)
            .str()
            .contains(left_str.lit(), true)
            .eq(false),
        _ => {
            let h = &mut vm.hint;
            h.white("Only support in/not in for string")
                .print_and_exit();
        }
    };
    vm.source.filter(e);
    Ok(Value::None)
}

fn compare_in_and_not_in_dataframe<'a>(
    vm: &'a mut Vm,
    left: Value,
    right: Value,
    op: &CmpOp,
) -> FilterxResult<Value> {
    let left_col = match &left {
        Value::Name(l) => l.name.clone(),
        _ => {
            return Err(FilterxError::RuntimeError(
                "Only support in/not in for column".to_string(),
            ));
        }
    };
    let right = match &right {
        Value::Str(path_repr) => util::handle_file(&path_repr)?,
        Value::List(_l) => right.clone(),
        _ => {
            let h = &mut vm.hint;
            h.white("Only support File/List for in/not in")
                .print_and_exit();
        }
    };
    let df_root = vm.source.lazy();
    let left_df = df_root.collect()?;
    let left_col_type = left_df.column(&left.to_string())?.dtype();
    let right_col = match &right {
        Value::File(f) => f.select.clone(),
        Value::List(_l) => "__vitrual_column_filterx__".into(),
        _ => {
            unreachable!();
        }
    };

    let right_df = match &right {
        Value::List(l) => match left_col_type {
            DataType::Float32 | DataType::Float64 => {
                let mut v = Vec::new();
                for i in l.iter() {
                    match i {
                        Value::Float(f) => v.push(*f),
                        Value::Int(i) => v.push(*i as f64),
                        _ => {
                            return Err(FilterxError::RuntimeError(
                                "Column type is float, but value is not".to_string(),
                            ));
                        }
                    }
                }
                DataFrame::new(vec![Column::new(right_col.as_str().into(), v)])?
            }
            DataType::Int32 | DataType::Int64 => {
                let mut v = Vec::new();
                for i in l.iter() {
                    match i {
                        Value::Int(i) => v.push(*i),
                        Value::Float(f) => v.push(*f as i64),
                        _ => {
                            return Err(FilterxError::RuntimeError(
                                "Column type is int, but value is not".to_string(),
                            ));
                        }
                    }
                }
                DataFrame::new(vec![Column::new(right_col.as_str().into(), v)])?
            }
            DataType::String => {
                let mut v = Vec::new();
                for i in l.iter() {
                    match i {
                        Value::Str(s) => v.push(s.clone()),
                        Value::Int(i) => v.push(i.to_string()),
                        Value::Float(f) => v.push(f.to_string()),
                        _ => {
                            return Err(FilterxError::RuntimeError(
                                "Column type is string, but value is not".to_string(),
                            ));
                        }
                    }
                }
                DataFrame::new(vec![Column::new(right_col.as_str().into(), v)])?
            }
            _ => {
                return Err(FilterxError::RuntimeError(
                    "Only support int/float/string in list".to_string(),
                ));
            }
        },
        Value::File(f) => f.df.clone(),
        _ => {
            unreachable!();
        }
    };

    let left_on = [left_col.as_str()];
    let right_on = [right_col.as_str()];

    match op {
        CmpOp::In => {
            let df = left_df.join(&right_df, left_on, right_on, JoinArgs::new(JoinType::Semi))?;
            vm.source.update(df.lazy());
        }
        CmpOp::NotIn => {
            let df = left_df.join(&right_df, left_on, right_on, JoinArgs::new(JoinType::Anti))?;
            vm.source.update(df.lazy());
        }
        _ => unreachable!(),
    }

    Ok(Value::None)
}

fn compare_cond_expr_in_dataframe<'a>(
    vm: &'a mut Vm,
    left: Value,
    right: Value,
    op: &CmpOp,
) -> FilterxResult<Value> {
    let left_expr = left.expr()?;
    let right_expr = right.expr()?;
    let e = cond_expr_build(vm, left_expr, right_expr, op.clone())?;
    vm.source.filter(e);
    Ok(Value::None)
}

fn cond_expr_build<'a>(vm: &'a mut Vm, left: Expr, right: Expr, op: CmpOp) -> FilterxResult<Expr> {
    let e = match op {
        CmpOp::Eq => left.eq(right),
        CmpOp::NotEq => left.neq(right),
        CmpOp::Lt => left.lt(right),
        CmpOp::LtE => left.lt_eq(right),
        CmpOp::Gt => left.gt(right),
        CmpOp::GtE => left.gt_eq(right),
        _ => {
            let h = &mut vm.hint;
            h.white("Only support compare op : ==, !=, >, >=, <, <=")
                .print_and_exit();
        }
    };
    Ok(e)
}
