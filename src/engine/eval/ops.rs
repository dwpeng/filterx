use std::ops::Deref;

use polars::prelude::*;
use rustpython_parser::ast::located::CmpOp;

use super::super::ast;
use crate::engine::value::Value;
use crate::source::Source;
use crate::util;

use crate::engine::eval::Eval;
use crate::engine::vm::Vm;
use crate::{check_types, eval};
use crate::{FilterxError, FilterxResult};

impl<'a> Eval<'a> for ast::ExprUnaryOp {
    type Output = Value;
    fn eval(&self, vm: &'a mut Vm) -> FilterxResult<Self::Output> {
        if !check_types!(self.operand.deref(), Constant, Call, UnaryOp, Name, BinOp) {
            let h = &mut vm.hint;
            h.white("only support constant, call, unaryop, name, BinOp, ")
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
                .red(&format!("{:?}", self.operand.deref()))
                .print_and_exit();
        }

        let v = eval!(vm, self.operand.deref(), Constant, Call, UnaryOp, Name);
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
            Value::Name(_) | Value::Column(_) => {
                println!("{:?}", -v.expr()?);
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
        let pass = check_types!(self.left.deref(), Constant, Call, UnaryOp, Name, BinOp)
            && check_types!(self.right.deref(), Constant, Call, UnaryOp, Name, BinOp);

        if !pass {
            let h = &mut vm.hint;
            h.white("Only support constant and constant, column and constant, column and column")
                .print_and_exit();
        }

        let l = eval!(vm, self.left.deref(), Constant, Call, UnaryOp, Name, BinOp);
        let r = eval!(vm, self.right.deref(), Constant, Call, UnaryOp, Name, BinOp);

        match self.op {
            ast::Operator::Add | ast::Operator::Sub | ast::Operator::Mult | ast::Operator::Div => {}
            _ => {
                let h = &mut vm.hint;
                h.white("Only support binary op : +, -, *, /")
                    .print_and_exit();
            }
        }

        if l.is_const() && r.is_const() {
            let ret = match self.op {
                ast::Operator::Add => binop(l, r, ast::Operator::Add),
                ast::Operator::Sub => binop(l, r, ast::Operator::Sub),
                ast::Operator::Mult => binop(l, r, ast::Operator::Mult),
                ast::Operator::Div => binop(l, r, ast::Operator::Div),
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
                _ => {
                    unreachable!();
                }
            };
            return Ok(Value::Expr(ret));
        }

        match vm.source {
            Source::Dataframe(_) => {
                return binop_for_dataframe(l, r, self.op);
            }
        }
    }
}

fn binop(l: Value, r: Value, op: ast::Operator) -> Value {
    match op {
        ast::Operator::Add => match (l, r) {
            (Value::Int(l), Value::Int(r)) => Value::Int(l + r),
            (Value::Float(l), Value::Float(r)) => Value::Float(l + r),
            (Value::Str(l), Value::Str(r)) => Value::Str(l + &r),
            (Value::Float(l), Value::Int(r)) => Value::Float(l + r as f64),
            (Value::Int(l), Value::Float(r)) => Value::Float(l as f64 + r),
            _ => unreachable!(),
        },
        ast::Operator::Sub => match (l, r) {
            (Value::Int(l), Value::Int(r)) => Value::Int(l - r),
            (Value::Float(l), Value::Float(r)) => Value::Float(l - r),
            (Value::Float(l), Value::Int(r)) => Value::Float(l - r as f64),
            (Value::Int(l), Value::Float(r)) => Value::Float(l as f64 - r),
            _ => unreachable!(),
        },
        ast::Operator::Mult => match (l, r) {
            (Value::Int(l), Value::Int(r)) => Value::Int(l * r),
            (Value::Float(l), Value::Float(r)) => Value::Float(l * r),
            (Value::Float(l), Value::Int(r)) => Value::Float(l * r as f64),
            (Value::Int(l), Value::Float(r)) => Value::Float(l as f64 * r),
            _ => unreachable!(),
        },
        ast::Operator::Div => match (l, r) {
            (Value::Int(l), Value::Int(r)) => Value::Int(l / r),
            (Value::Float(l), Value::Float(r)) => Value::Float(l / r),
            (Value::Float(l), Value::Int(r)) => Value::Float(l / r as f64),
            (Value::Int(l), Value::Float(r)) => Value::Float(l as f64 / r),
            _ => unreachable!(),
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

        let pass = check_types!(left, Compare, BoolOp) && check_types!(self.values[1], Compare);
        if !pass {
            let h = &mut vm.hint;
            h.white("Only support chain compare, like a > 1 and a < 2")
                .print_and_exit();
        }
        let left = eval!(vm, left, Compare, BoolOp);
        let right = &self.values[1];
        let right = eval!(vm, right, Compare, BoolOp);
        vm.status.update_apply_lazy(vm_apply_lazy);

        match vm.source {
            Source::Dataframe(_) => {
                return boolop_in_dataframe(vm, &left, &right, self.op.clone());
            }
        }
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

    let df = vm.source.dataframe_mut_ref().unwrap();

    match op {
        ast::BoolOp::And => match (l, r) {
            (_, _) => {
                let lazy = df.lazy.clone();
                let lazy = lazy.filter(l.expr()?.and(r.clone().expr()?));
                df.lazy = lazy;
            }
        },
        ast::BoolOp::Or => match (l, r) {
            (_, _) => {
                let lazy = df.lazy.clone();
                let lazy = lazy.filter(l.expr()?.or(r.expr()?));
                df.lazy = lazy;
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

        let pass = check_types!(
            self.left.deref(),
            Constant,
            Call,
            UnaryOp,
            BinOp,
            BoolOp,
            Name
        );

        if !pass {
            let h = &mut vm.hint;
            h.white("In `in` compare, left must be column, or string constant, ")
                .white("example: ")
                .cyan("'a' in a")
                .white(", ")
                .cyan("a in (1, 2, 3)")
                // .white(", ")
                // .cyan("a in file('path')")
                .print_and_exit();
        }

        let right = &self.comparators[0];
        let pass = check_types!(right, Constant, Call, UnaryOp, BinOp, BoolOp, Name, Tuple);

        if !pass {
            let h = &mut vm.hint;
            h.white("In `in` compare, right must be column, or constant, ")
                .white("example: ")
                .cyan("'a' in a")
                .white(", ")
                .cyan("a in (1, 2, 3)")
                // .white(", ")
                // .cyan("a in file('path')")
                .print_and_exit();
        }

        let left = eval!(
            vm,
            self.left.deref(),
            Constant,
            Call,
            UnaryOp,
            BinOp,
            BoolOp,
            Name
        );
        let op = &self.ops[0];
        let right = eval!(vm, right, Constant, Call, UnaryOp, BinOp, BoolOp, Name, Tuple);
        match vm.source {
            Source::Dataframe(_) => {
                return compare_in_datarame(vm, left, right, op);
            }
        }
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
        Value::Column(c) => c.col_name.clone(),
        _ => unreachable!(),
    };
    let df = vm.source.dataframe_mut_ref().unwrap();
    let lazy = df.lazy.clone();
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
    let lazy = lazy.filter(e);
    df.lazy = lazy;
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
    let df_root = vm.source.dataframe_mut_ref().unwrap();

    let left_df = df_root.lazy.clone().collect()?;
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
                DataFrame::new(vec![Series::new(right_col.as_str().into(), v)])?
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
                DataFrame::new(vec![Series::new(right_col.as_str().into(), v)])?
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
                DataFrame::new(vec![Series::new(right_col.as_str().into(), v)])?
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
            df_root.lazy = df.lazy();
        }
        CmpOp::NotIn => {
            let df = left_df.join(&right_df, left_on, right_on, JoinArgs::new(JoinType::Anti))?;
            df_root.lazy = df.lazy();
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
    let df = vm.source.dataframe_mut_ref().unwrap();
    let mut lazy = df.lazy.clone();
    lazy = lazy.filter(e);
    df.lazy = lazy;
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
