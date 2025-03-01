use std::ops::Deref;

use polars::prelude::*;
use rustpython_parser::ast::located::CmpOp;

use super::super::ast;

use crate::eval::Eval;
use crate::vm::Vm;
pub use crate::{eval, eval_col, execuable};
use filterx_core::{value::Value, FilterxResult, Hint};

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
            Value::Name(_) => {
                return Ok(Value::named_expr(None, -(v.expr()?)));
            }
            Value::NamedExpr(e) => {
                return Ok(Value::named_expr(e.name.clone(), -e.expr.clone()));
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

        let lname = l.name();
        let rname = r.name();

        if let Some(name) = lname {
            vm.source_mut().has_column(name);
        }

        if let Some(name) = rname {
            vm.source_mut().has_column(name);
        }

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
            return Ok(Value::named_expr(None, ret.expr()?));
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
            return Ok(Value::named_expr(None, ret));
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
    Ok(Value::named_expr(None, ret))
}

/// and, or
impl<'a> Eval<'a> for ast::ExprBoolOp {
    type Output = Value;
    fn eval(&self, vm: &'a mut Vm) -> FilterxResult<Self::Output> {
        execuable!(vm, "`and` or `or`");
        let source = vm.source_mut();
        source.enter_and_ctx();
        let left = &self.values[0];
        let left = eval!(
            vm,
            left,
            "Only support chain compare, like a > 1 and a < 2",
            Compare,
            BoolOp
        );
        let right = &self.values[1];
        // let right_exprs = vec![];
        // let ops = vec![];
        let right = eval!(
            vm,
            right,
            "Only support chain compare, like a > 1 and a < 2",
            Compare,
            BoolOp
        );
        boolop_in_dataframe(vm, &left, &right, self.op.clone())
    }
}

fn boolop_in_dataframe<'a>(
    vm: &'a mut Vm,
    l: &Value,
    r: &Value,
    op: ast::BoolOp,
) -> FilterxResult<Value> {
    let source = vm.source_mut();
    let mut lazy = source.lazy();
    match op {
        ast::BoolOp::And => match (l, r) {
            (_, _) => {
                lazy = lazy.filter(l.expr()?.and(r.clone().expr()?));
            }
        },
        ast::BoolOp::Or => match (l, r) {
            (_, _) => {
                lazy = lazy.filter(l.expr()?.or(r.expr()?));
            }
        },
    }
    source.update(lazy);
    source.exit_and_ctx();
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
        execuable!(vm, "`>=`, `<=`, `==`, `!=`, `<`, `>`, `in`, `not in`");
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

        let lname = left.name();
        let rname = right.name();

        if let Some(name) = lname {
            vm.source_mut().has_column(name);
        }

        if let Some(name) = rname {
            vm.source_mut().has_column(name);
        }

        compare_in(vm, left, right, op)
    }
}

fn compare_in<'a>(vm: &'a mut Vm, left: Value, right: Value, op: &CmpOp) -> FilterxResult<Value> {
    match op {
        CmpOp::In | CmpOp::NotIn => {
            if left.is_str() && right.is_column() {
                return str_in_col(vm, left, right, op);
            }

            if left.is_column() && !left.is_str() && right.is_list() {
                return col_in_list(vm, left, right, op);
            }

            let h = &mut vm.hint;
            h.white("Only two kinds of compare are supported: ")
                .white("column in list: ")
                .cyan("a in ('b', 'c')")
                .white(" or ")
                .white("string in column: ")
                .cyan("'a' in a")
                .print_and_exit();
        }
        CmpOp::Eq | CmpOp::NotEq | CmpOp::Lt | CmpOp::LtE | CmpOp::Gt | CmpOp::GtE => {
            let l = left.is_const() | left.is_keyword();
            let r = right.is_const() | right.is_keyword();
            if l && r {
                let h = &mut vm.hint;
                h.white("Only support compare between column and constant")
                    .print_and_exit();
            }
            return compare_cond_expr_in_dataframe(vm, left, right, op);
        }
        _ => {
            let h = &mut vm.hint;
            h.white("Only support compare op : ==, !=, >, >=, <, <=")
                .print_and_exit();
        }
    };
}

fn col_in_list<'a>(vm: &'a mut Vm, left: Value, right: Value, op: &CmpOp) -> FilterxResult<Value> {
    let left_col: &str = left.column().unwrap();
    vm.source().has_column(left_col);
    let right_list: Vec<Value> = right.list().unwrap();
    if right_list.is_empty() {
        let h = &mut vm.hint;
        h.white("List can't be empty").print_and_exit();
    }
    let columns = vm.source().columns().unwrap();
    let left_col_type = columns
        .iter()
        .find(|(name, _)| *name == left_col)
        .unwrap()
        .1;

    let right_expr: Expr;
    match left_col_type {
        DataType::Int64
        | DataType::Int32
        | DataType::Int16
        | DataType::Int8
        | DataType::UInt64
        | DataType::UInt32
        | DataType::UInt16
        | DataType::UInt8 => {
            let right_list = right_list.iter().map(|v| v.int()).collect::<Vec<_>>();
            for v in &right_list {
                if v.is_err() {
                    let h = &mut vm.hint;
                    h.white("List must be int type, because left col's type is int.")
                        .print_and_exit();
                }
            }
            let mut right_values = Vec::with_capacity(right_list.len());
            for v in right_list {
                right_values.push(v.unwrap());
            }
            let right_values = Series::new("__X_x__".into(), right_values);
            right_expr = right_values.lit();
        }
        DataType::Float32 | DataType::Float64 => {
            let right_list = right_list.iter().map(|v| v.float()).collect::<Vec<_>>();
            for v in &right_list {
                if v.is_err() {
                    let h = &mut vm.hint;
                    h.white("List must be float type, because left col's type is float.")
                        .print_and_exit();
                }
            }
            let mut right_values = Vec::with_capacity(right_list.len());
            for v in right_list {
                right_values.push(v.unwrap());
            }
            let right_values = Series::new("__X_x__".into(), right_values);
            right_expr = right_values.lit();
        }
        DataType::String => {
            let right_list = right_list.iter().map(|v| v.string()).collect::<Vec<_>>();
            for v in &right_list {
                if v.is_err() {
                    let h = &mut vm.hint;
                    h.white("List must be string type, because left col's type is string.")
                        .print_and_exit();
                }
            }
            let mut right_values = Vec::with_capacity(right_list.len());
            for v in right_list {
                right_values.push(v.unwrap());
            }
            let right_values = Series::new("__X_x__".into(), right_values);
            right_expr = right_values.lit();
        }
        _ => {
            let h = &mut vm.hint;
            h.white("Only support int, float, string type")
                .print_and_exit();
        }
    }
    let left_expr = left.expr()?;
    let e = match op {
        CmpOp::In => left_expr.is_in(right_expr),
        CmpOp::NotIn => left_expr.is_in(right_expr).not(),
        _ => {
            unreachable!();
        }
    };
    vm.source_mut().filter(e);
    Ok(Value::None)
}

fn str_in_col<'a>(vm: &'a mut Vm, left: Value, right: Value, op: &CmpOp) -> FilterxResult<Value> {
    let left_str = left.string().unwrap();
    let right_col: &str = right.column().unwrap();
    vm.source().has_column(right_col);
    let e = col(right_col).str().contains(left_str.lit(), true);
    let e = match op {
        CmpOp::In => e.eq(true.lit()),
        CmpOp::NotIn => e.eq(false.lit()),
        _ => {
            let h = &mut vm.hint;
            h.white("Only support in/not in for string")
                .print_and_exit();
        }
    };
    vm.source_mut().filter(e.clone());
    Ok(Value::named_expr(None, e))
}

fn compare_cond_expr_in_dataframe<'a>(
    vm: &'a mut Vm,
    left: Value,
    right: Value,
    op: &CmpOp,
) -> FilterxResult<Value> {
    let left_expr = left.expr()?;
    let right_expr = right.expr()?;
    let e = match op {
        CmpOp::Eq => match (left, right) {
            (Value::Null, _) => right_expr.is_null(),
            (_, Value::Null) => left_expr.is_null(),
            _ => left_expr.eq(right_expr),
        },
        CmpOp::NotEq => match (left, right) {
            (Value::Null, _) => right_expr.is_not_null(),
            (_, Value::Null) => left_expr.is_not_null(),
            _ => left_expr.neq(right_expr),
        },
        CmpOp::Lt => left_expr.lt(right_expr),
        CmpOp::LtE => left_expr.lt_eq(right_expr),
        CmpOp::Gt => left_expr.gt(right_expr),
        CmpOp::GtE => left_expr.gt_eq(right_expr),
        _ => {
            let h = &mut vm.hint;
            h.white("Only support compare op : ==, !=, >, >=, <, <=")
                .print_and_exit();
        }
    };
    vm.source_mut().filter(e.clone());
    Ok(Value::named_expr(None, e))
}
