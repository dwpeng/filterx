use std::ops::Deref;

use polars::prelude::*;
use rustpython_parser::ast::located::CmpOp;

use super::super::ast;
use crate::engine::value::Value;
use crate::util;

use crate::engine::eval::Eval;
use crate::engine::vm::Vm;
use crate::{FilterxError, FilterxResult};

impl<'a> Eval<'a> for ast::ExprUnaryOp {
    type Output = Value;
    fn eval(&self, vm: &'a mut Vm) -> FilterxResult<Self::Output> {
        let v = match *self.operand {
            ast::Expr::Constant(ref c) => c.eval(vm)?, // -1
            ast::Expr::Call(ref c) => c.eval(vm)?,     // -col(1)
            ast::Expr::UnaryOp(ref u) => u.eval(vm)?,  // --1
            ast::Expr::Name(ref n) => n.eval(vm)?,     // -a
            _ => {
                return Err(FilterxError::RuntimeError(
                    "Only support int/float/column to apply unary op".to_string(),
                ))
            }
        };

        match self.op {
            ast::UnaryOp::Invert | ast::UnaryOp::Not | ast::UnaryOp::UAdd => {
                return Err(FilterxError::RuntimeError(
                    "Only support unary op : -".to_string(),
                ))
            }
            _ => {}
        }

        match &v {
            Value::Int(_) | Value::Float(_) => {
                let r = unary(v, self.op)?;
                return Ok(r);
            }

            Value::Column(_) => {
                let expr = v.expr()?;
                return Ok(Value::Expr(-expr));
            }

            Value::Expr(e) => {
                return Ok(Value::Expr(-e.clone()));
            }

            _ => {
                return Err(FilterxError::RuntimeError(
                    "Only support int/float/column to apply unary op".to_string(),
                ))
            }
        }
    }
}

fn unary(v: Value, op: ast::UnaryOp) -> FilterxResult<Value> {
    match v {
        Value::Int(i) => match op {
            ast::UnaryOp::USub => return Ok(Value::Int(-i)),
            _ => unreachable!(),
        },
        Value::Float(f) => match op {
            ast::UnaryOp::USub => return Ok(Value::Float(-f)),
            _ => unreachable!(),
        },
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
        let l = match *self.left {
            // Value::Int(i) => Value::Int(i),
            // Value::Float(f) => Value::Float(f),
            // Value::Str(s) => Value::Str(s),
            ast::Expr::Constant(ref c) => c.eval(vm)?,
            // ast::Expr::Call(ref c) => c.eval(vm)?,
            ast::Expr::UnaryOp(ref u) => u.eval(vm)?,
            // Value::Column(c) => Value::Column(c),
            ast::Expr::Name(ref n) => n.eval(vm)?,
            // a+b
            ast::Expr::BinOp(ref b) => b.eval(vm)?,
            _ => {
                return Err(FilterxError::RuntimeError(
                    "Only support constant and Column".to_string(),
                ))
            }
        };

        let r = match *self.right {
            ast::Expr::Constant(ref c) => c.eval(vm)?,
            // ast::Expr::Call(ref c) => c.eval(vm)?,
            ast::Expr::UnaryOp(ref u) => u.eval(vm)?,
            ast::Expr::Name(ref n) => n.eval(vm)?,
            ast::Expr::BinOp(ref b) => b.eval(vm)?,
            _ => {
                return Err(FilterxError::RuntimeError(
                    "Only support constant and Column".to_string(),
                ))
            }
        };

        if l.is_const() && r.is_const() {
            let ret = match self.op {
                ast::Operator::Add => binop(l, r, ast::Operator::Add),
                ast::Operator::Sub => binop(l, r, ast::Operator::Sub),
                ast::Operator::Mult => binop(l, r, ast::Operator::Mult),
                ast::Operator::Div => binop(l, r, ast::Operator::Div),
                _ => {
                    return Err(FilterxError::RuntimeError(
                        "Only support binary op : +, -, *, /".to_string(),
                    ))
                }
            };
            return Ok(ret);
        }

        let ret = match self.op {
            ast::Operator::Add => l.expr()? + r.expr()?,
            ast::Operator::Sub => l.expr()? - r.expr()?,
            ast::Operator::Mult => l.expr()? * r.expr()?,
            ast::Operator::Div => l.expr()? / r.expr()?,
            _ => {
                return Err(FilterxError::RuntimeError(
                    "Only support binary op : +, -, *, /".to_string(),
                ))
            }
        };
        Ok(Value::Expr(ret))
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

/// and, or
impl<'a> Eval<'a> for ast::ExprBoolOp {
    type Output = Value;
    fn eval(&self, vm: &'a mut Vm) -> FilterxResult<Self::Output> {
        let left = &self.values[0];
        let vm_apply_lazy = vm.apply_lazy;
        vm.apply_lazy = false;
        let left = match left {
            ast::Expr::Compare(ref c) => c.eval(vm)?,
            ast::Expr::BoolOp(ref b) => b.eval(vm)?,
            _ => {
                return Err(FilterxError::RuntimeError(
                    "Only support chain compare, like a > 1 and a < 2".to_string(),
                ))
            }
        };
        let right = &self.values[1];
        let right = match right {
            ast::Expr::Compare(ref c) => c.eval(vm)?,
            ast::Expr::BoolOp(ref b) => b.eval(vm)?,
            _ => {
                return Err(FilterxError::RuntimeError(
                    "Only support chain compare, like a > 1 and a < 2".to_string(),
                ))
            }
        };
        vm.apply_lazy = vm_apply_lazy;
        let v = match self.op {
            ast::BoolOp::And => boolop(vm, &left, &right, ast::BoolOp::And)?,
            ast::BoolOp::Or => boolop(vm, &left, &right, ast::BoolOp::Or)?,
        };
        Ok(v)
    }
}

fn boolop<'a>(vm: &'a mut Vm, l: &Value, r: &Value, op: ast::BoolOp) -> FilterxResult<Value> {
    let vm_apply_lazy = vm.apply_lazy;
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
                let lazy = vm.lazy.clone();
                let lazy = lazy.filter(l.expr()?.and(r.clone().expr()?));
                vm.lazy = lazy;
            }
        },
        ast::BoolOp::Or => match (l, r) {
            (_, _) => {
                let lazy = vm.lazy.clone();
                let lazy = lazy.filter(l.expr()?.or(r.expr()?));
                vm.lazy = lazy;
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
        let left = &self.left;
        let left = match left.deref() {
            ast::Expr::Constant(ref c) => c.eval(vm)?,
            ast::Expr::Call(ref c) => c.eval(vm)?,
            ast::Expr::UnaryOp(ref u) => u.eval(vm)?,
            ast::Expr::BinOp(ref b) => b.eval(vm)?,
            ast::Expr::BoolOp(ref b) => b.eval(vm)?,
            ast::Expr::Name(ref n) => n.eval(vm)?,
            ast::Expr::Compare(ref c) => c.eval(vm)?,
            _ => {
                return Err(FilterxError::RuntimeError(
                    "Only support constant".to_string(),
                ))
            }
        };

        if self.ops.len() > 1 {
            return Err(FilterxError::RuntimeError(
                "Only support one compare op, like a > 1. If you want to chain compare, use `and` or `or`"
                    .to_string(),
            ));
        }

        let right = &self.comparators[0];
        let op = &self.ops[0];

        let right = match right {
            ast::Expr::Constant(ref c) => c.eval(vm)?,
            ast::Expr::Call(ref c) => c.eval(vm)?,
            ast::Expr::UnaryOp(ref u) => u.eval(vm)?,
            ast::Expr::BinOp(ref b) => b.eval(vm)?,
            ast::Expr::BoolOp(ref b) => b.eval(vm)?,
            ast::Expr::Compare(ref c) => c.eval(vm)?,
            ast::Expr::Name(ref n) => n.eval(vm)?,
            ast::Expr::Tuple(ref t) => t.eval(vm)?,
            _ => {
                return Err(FilterxError::RuntimeError(
                    "only support constant".to_string(),
                ))
            }
        };

        match op {
            CmpOp::In | CmpOp::NotIn => {
                if vm.apply_lazy {
                    return compare_in_and_notin(vm, left, right, op);
                } else {
                    return Err(FilterxError::RuntimeError(
                        "in/not in operator can't be used with and/or".to_string(),
                    ));
                }
            }
            CmpOp::Eq | CmpOp::NotEq | CmpOp::Lt | CmpOp::LtE | CmpOp::Gt | CmpOp::GtE => {
                if vm.apply_lazy {
                    return compare_cond(vm, left, right, op);
                } else {
                    return compare_cond_expr(vm, left, right, op);
                }
            }
            _ => {
                return Err(FilterxError::RuntimeError(
                    "Only support compare op : ==, !=, >, >=, <, <=, in, not in".to_string(),
                ))
            }
        };
    }
}

fn compare_in_and_notin<'a>(
    vm: &'a mut Vm,
    left: Value,
    right: Value,
    op: &CmpOp,
) -> FilterxResult<Value> {
    let left_col = match &left {
        Value::Column(l) => l.col_name.clone(),
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
            return Err(FilterxError::RuntimeError(
                "Only support File/List for in/not in".to_string(),
            ));
        }
    };

    let left_df = vm.lazy.clone().collect()?;
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
            vm.lazy = df.lazy();
        }
        CmpOp::NotIn => {
            let df = left_df.join(&right_df, left_on, right_on, JoinArgs::new(JoinType::Anti))?;
            vm.lazy = df.lazy();
        }
        _ => unreachable!(),
    }

    Ok(Value::None)
}

fn compare_cond<'a>(vm: &'a mut Vm, left: Value, right: Value, op: &CmpOp) -> FilterxResult<Value> {
    // ensure left is column
    if !left.is_column() && !right.is_column() {
        return Err(FilterxError::RuntimeError(
            "Only support compare for column".to_string(),
        ));
    }

    let mut left = left;
    let mut right = right;
    let mut op = op.clone();
    if !left.is_column() {
        (left, right) = (right, left);
        match op {
            CmpOp::Lt => op = CmpOp::Gt,
            CmpOp::LtE => op = CmpOp::GtE,
            CmpOp::Gt => op = CmpOp::Lt,
            CmpOp::GtE => op = CmpOp::LtE,
            _ => {}
        };
    }

    let left_col = match &left {
        Value::Column(l) => l.col_name.clone(),
        _ => unreachable!(),
    };

    let mut lazy = vm.lazy.clone();
    match op {
        CmpOp::Eq => {
            lazy = lazy.filter(col(left_col).eq(right.expr()?));
        }
        CmpOp::NotEq => {
            lazy = lazy.filter(col(left_col).neq(right.expr()?));
        }
        CmpOp::Lt => {
            lazy = lazy.filter(col(left_col).lt(right.expr()?));
        }
        CmpOp::LtE => {
            lazy = lazy.filter(col(left_col).lt_eq(right.expr()?));
        }
        CmpOp::Gt => {
            lazy = lazy.filter(col(left_col).gt(right.expr()?));
        }
        CmpOp::GtE => {
            lazy = lazy.filter(col(left_col).gt_eq(right.expr()?));
        }
        _ => {
            return Err(FilterxError::RuntimeError(
                "Only support compare op : ==, !=, >, >=, <, <=".to_string(),
            ));
        }
    }
    vm.lazy = lazy;
    Ok(Value::None)
}

fn compare_cond_expr<'a>(
    _vm: &'a mut Vm,
    left: Value,
    right: Value,
    op: &CmpOp,
) -> FilterxResult<Value> {
    // ensure left is column
    if !left.is_column() && !right.is_column() {
        return Err(FilterxError::RuntimeError(
            "Only support compare for column".to_string(),
        ));
    }

    let mut left = left;
    let mut right = right;
    let mut op = op.clone();
    if !left.is_column() {
        (left, right) = (right, left);
        match op {
            CmpOp::Lt => op = CmpOp::Gt,
            CmpOp::LtE => op = CmpOp::GtE,
            CmpOp::Gt => op = CmpOp::Lt,
            CmpOp::GtE => op = CmpOp::LtE,
            _ => {}
        };
    }

    let left_col = match &left {
        Value::Column(l) => l.col_name.clone(),
        _ => {
            return Err(FilterxError::RuntimeError(
                "Only support compare for column".to_string(),
            ));
        }
    };

    let e = match op {
        CmpOp::Eq => col(left_col).eq(right.expr()?),
        CmpOp::NotEq => col(left_col).neq(right.expr()?),
        CmpOp::Lt => col(left_col).lt(right.expr()?),
        CmpOp::LtE => col(left_col).lt_eq(right.expr()?),
        CmpOp::Gt => col(left_col).gt(right.expr()?),
        CmpOp::GtE => col(left_col).gt_eq(right.expr()?),
        _ => {
            return Err(FilterxError::RuntimeError(
                "Only support compare op : ==, !=, >, >=, <, <=".to_string(),
            ));
        }
    };
    Ok(Value::Expr(e))
}
