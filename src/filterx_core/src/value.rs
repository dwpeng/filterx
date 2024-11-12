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
    Item(Item),
    Ident((String, Box<Value>)),
    AttrMethod(AttrMethod),
    File(File),
    NamedExpr(NamedExpr),
    // Slice(Slice),
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
    pub fn int(&self) -> i64 {
        match self {
            Value::Int(i) => *i,
            _ => panic!("not a int"),
        }
    }

    pub fn float(&self) -> f64 {
        match self {
            Value::Float(f) => *f,
            Value::Int(i) => *i as f64,
            _ => panic!("not a float"),
        }
    }

    pub fn expr(&self) -> FilterxResult<Expr> {
        let expr = match self {
            Value::Float(f) => f.lit(),
            Value::Int(i) => i.lit(),
            Value::Str(s) => s.clone().lit(),
            Value::Item(c) => col(c.col_name.clone()),
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
            Value::Item(c) => Ok(c.col_name.to_owned()),
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
            Value::Item(c) => Ok(c.col_name.as_str()),
            Value::Name(n) => Ok(n.name.as_str()),
            Value::Str(s) => Ok(s.as_str()),
            Value::NamedExpr(e) => match e.name {
                Some(ref name) => Ok(name.as_str()),
                None => return Err(FilterxError::RuntimeError("Can't find column name.".into())),
            },
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

    pub fn is_column(&self) -> bool {
        match self {
            Value::Item(_) => true,
            Value::Name(_) => true,
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

    pub fn free(self) -> () {}
}

#[derive(Debug, Clone)]
pub struct Slice {
    pub start: Option<Box<Value>>,
    pub end: Option<Box<Value>>,
}

impl PartialEq for Slice {
    fn eq(&self, other: &Self) -> bool {
        self.start == other.start && self.end == other.end
    }
}

impl Default for Slice {
    fn default() -> Self {
        Slice {
            start: None,
            end: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Item {
    pub col_name: String,
    pub data_type: Option<DataType>,
}

impl Item {
    pub fn new(col_name: String) -> Self {
        Item {
            col_name,
            data_type: None,
        }
    }

    pub fn as_str(&self) -> &str {
        &self.col_name
    }
}

impl Default for Item {
    fn default() -> Self {
        Item {
            col_name: String::new(),
            data_type: None,
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct AttrMethod {
    pub col: Item,
    pub method: String,
    pub value: Vec<Value>,
}

#[derive(Debug, Clone)]
pub struct File {
    pub file_name: String,
    pub seprarator: char,
    pub select: String,
    pub df: DataFrame,
}

impl PartialEq for File {
    fn eq(&self, other: &Self) -> bool {
        self.file_name == other.file_name
    }
}

impl File {
    pub fn new(file_name: String, seprarator: char, select: String, target: DataFrame) -> Self {
        File {
            file_name,
            seprarator,
            select,
            df: target,
        }
    }
}

impl Default for File {
    fn default() -> Self {
        File {
            file_name: String::new(),
            seprarator: ',',
            select: String::new(),
            df: DataFrame::empty(),
        }
    }
}

#[allow(unused)]
fn string_slice(slice: &Slice) -> String {
    let mut s = String::from("[");
    if let Some(ref start) = slice.start {
        s.push_str(&start.to_string());
    }
    s.push_str(":");
    if let Some(ref end) = slice.end {
        s.push_str(&end.to_string());
    }
    s.push_str("]");
    s
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
            Value::AttrMethod(attr) => {
                let s = format!(
                    "AttrMethod({}.{} {:?})",
                    attr.col.col_name, attr.method, attr.value
                );
                s
            }
            Value::File(f) => f.file_name.clone(),
            Value::Item(c) => c.col_name.clone(),
            Value::Ident(i) => {
                let mut s = String::from("(");
                s.push_str(&i.0);
                s.push_str(": ");
                s.push_str(&i.1.to_string());
                s.push_str(")");
                s
            }
            Value::Null => String::from("Null"),
            Value::Na => String::from("Na"),
            Value::None => String::from("None"),
            // Value::Slice(s) => {
            //     let mut str = String::from("Slice(");
            //     str.push_str(&string_slice(s));
            //     str.push_str(")");
            //     str
            // }
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Value({})", self.to_string())
    }
}
