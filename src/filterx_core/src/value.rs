use std::ops::Deref;

use crate::{FilterxError, FilterxResult, Hint};
use polars::prelude::*;
use rustpython_parser::ast::bigint::{BigInt, Sign};

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Int(i64),
    Float(f64),
    Bool(bool),
    Str(String),
    Name(Name),
    List(Vec<Value>),
    NamedExpr(NamedExpr),
    Null,
    Na,
    None,
}

impl Value {
    pub fn named_expr(name: Option<String>, expr: Expr) -> Self {
        Value::NamedExpr(NamedExpr { name, expr })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NamedExpr {
    pub name: Option<String>,
    pub expr: Expr,
}

impl Literal for Value {
    fn lit(self) -> Expr {
        match self {
            Value::Float(f) => Expr::Literal(LiteralValue::Float64(f)),
            Value::Int(i) => Expr::Literal(LiteralValue::Int64(i)),
            Value::Str(s) => Expr::Literal(LiteralValue::String(s.into())),
            _ => Expr::Literal(LiteralValue::Null),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum NameContext {
    Load,
    Store,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Name {
    pub name: String,
    pub ctx: NameContext,
}

impl Deref for Name {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        &self.name
    }
}

impl From<Name> for Value {
    fn from(n: Name) -> Self {
        Value::Name(n)
    }
}

impl From<i64> for Value {
    fn from(i: i64) -> Self {
        Value::Int(i)
    }
}

impl From<f64> for Value {
    fn from(f: f64) -> Self {
        Value::Float(f)
    }
}

impl Into<f64> for Value {
    fn into(self) -> f64 {
        match self {
            Value::Float(f) => f,
            _ => panic!("not a float"),
        }
    }
}

impl From<BigInt> for Value {
    fn from(i: BigInt) -> Self {
        let u64 = i.to_u64_digits();
        if u64.1.len() == 1 {
            match u64.0 {
                Sign::Minus => Value::Int(-(u64.1[0] as i64)),
                Sign::Plus => Value::Int(u64.1[0] as i64),
                _ => panic!("unsupported type"),
            }
        } else if u64.1.len() == 0 {
            Value::Int(0)
        } else {
            panic!("unsupported type")
        }
    }
}

impl Value {
    pub fn int(&self) -> FilterxResult<i64> {
        match self {
            Value::Int(i) => Ok(*i),
            _ => Err(FilterxError::RuntimeError(format!(
                "Can't convert {:?} to int",
                self
            ))),
        }
    }

    pub fn float(&self) -> FilterxResult<f64> {
        match self {
            Value::Float(f) => Ok(*f),
            Value::Int(i) => Ok(*i as f64),
            _ => Err(FilterxError::RuntimeError(format!(
                "Can't convert {:?} to float",
                self
            ))),
        }
    }

    pub fn expr(&self) -> FilterxResult<Expr> {
        let expr = match self {
            Value::Float(f) => f.lit(),
            Value::Int(i) => i.lit(),
            Value::Str(s) => s.clone().lit(),
            Value::Name(n) => col(n.name.clone()),
            Value::NamedExpr(e) => e.expr.clone(),
            Value::Null => Expr::Literal(LiteralValue::Null),
            Value::Na => Expr::Literal(LiteralValue::Null),
            Value::None => return Err(FilterxError::RuntimeError("function return None".into())),
            _ => return Err(FilterxError::RuntimeError("Can't convert to expr.".into())),
        };
        Ok(expr)
    }

    pub fn text(&self) -> FilterxResult<String> {
        match self {
            Value::Str(s) => Ok(s.to_owned()),
            Value::Name(n) => Ok(n.name.to_owned()),
            _ => {
                return Err(FilterxError::RuntimeError(
                    "Only Str or Column can convert to text".into(),
                ))
            }
        }
    }

    pub fn string(&self) -> FilterxResult<String> {
        match self {
            Value::Str(s) => Ok(s.to_owned()),
            _ => {
                return Err(FilterxError::RuntimeError(
                    "Only Str can convert to string".into(),
                ))
            }
        }
    }

    pub fn u32(&self) -> FilterxResult<u32> {
        match self {
            Value::Int(i) => Ok(*i as u32),
            _ => Err(FilterxError::RuntimeError(
                "Only Int can convert to u32".into(),
            )),
        }
    }

    pub fn column<'a>(&'a self) -> FilterxResult<&'a str> {
        match self {
            Value::Name(n) => Ok(n.name.as_str()),
            Value::Str(s) => Ok(s.as_str()),
            Value::NamedExpr(e) => match e.name {
                Some(ref name) => Ok(name.as_str()),
                None => return Err(FilterxError::RuntimeError("Can't find column name.".into())),
            },
            Value::Int(i) => {
                let mut h = Hint::new();
                h.white("Use ")
                    .green(&format!("col({})", i))
                    .white(" to access column by index.")
                    .print_and_exit()
            }
            _ => {
                let mut h = Hint::new();
                h.white("Expected the following types as a column name: ")
                    .cyan("name, string")
                    .white(" or ")
                    .cyan("function which returns a column name")
                    .print_and_exit();
            }
        }
    }

    pub fn r#const(&self) -> FilterxResult<Value> {
        match self {
            Value::Int(i) => Ok(Value::Int(*i)),
            Value::Float(f) => Ok(Value::Float(*f)),
            Value::Str(s) => Ok(Value::Str(s.clone())),
            _ => Err(FilterxError::RuntimeError(
                "Only Int, Float or Str can convert to const".into(),
            )),
        }
    }

    pub fn list(&self) -> FilterxResult<Vec<Value>> {
        match self {
            Value::List(l) => Ok(l.clone()),
            _ => Err(FilterxError::RuntimeError(
                "Only List can convert to list".into(),
            )),
        }
    }

    pub fn is_column(&self) -> bool {
        match self {
            Value::Name(_) => true,
            Value::NamedExpr(_) => true,
            Value::Str(_) => true,
            _ => false,
        }
    }

    pub fn is_const(&self) -> bool {
        match self {
            Value::Int(_) | Value::Float(_) | Value::Str(_) => true,
            _ => false,
        }
    }

    pub fn is_str(&self) -> bool {
        match self {
            Value::Str(_) => true,
            _ => false,
        }
    }

    pub fn is_expr(&self) -> bool {
        match self {
            Value::NamedExpr(_) => true,
            _ => false,
        }
    }

    pub fn is_list(&self) -> bool {
        match self {
            Value::List(_) => true,
            _ => false,
        }
    }
}

impl Value {
    pub fn to_string(&self) -> String {
        match self {
            Value::Int(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Str(s) => format!("'{}'", s),
            Value::NamedExpr(e) => {
                let s = format!("NamedExpr(name={:?}, expr={:?})", e.name, e.expr);
                s
            }
            Value::List(l) => {
                let mut s = String::from("[");
                for (i, v) in l.iter().enumerate() {
                    s.push_str(&v.to_string());
                    if i < l.len() - 1 {
                        s.push_str(", ");
                    }
                }
                s.push_str("]");
                s
            }
            Value::Name(n) => n.to_string(),
            Value::Null => String::from("Null"),
            Value::Na => String::from("Na"),
            Value::None => String::from("None"),
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Value({})", self.to_string())
    }
}
