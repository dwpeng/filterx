use crate::{FilterxError, FilterxResult};
use polars::prelude::*;
use rustpython_parser::ast::bigint::{BigInt, Sign};

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Int(i64),
    Float(f64),
    Bool(bool),
    Str(String),
    List(Vec<Value>),
    Column(Column),
    Ident((String, Box<Value>)),
    AttrMethod(AttrMethod),
    File(File),
    Expr(Expr),
    // Slice(Slice),
    None,
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
            Value::Column(c) => col(c.col_name.clone()),
            Value::Expr(e) => e.clone(),
            Value::None => return Err(FilterxError::RuntimeError("function return None".into())),
            _ => return Err(FilterxError::RuntimeError("Can't convert to expr.".into())),
        };
        Ok(expr)
    }

    pub fn text(&self) -> FilterxResult<String> {
        match self {
            Value::Str(s) => Ok(s.to_owned()),
            Value::Column(c) => Ok(c.col_name.to_owned()),
            _ => {
                return Err(FilterxError::RuntimeError(
                    "Only Str or Column can convert to text".into(),
                ))
            }
        }
    }

    pub fn string(&self) -> FilterxResult<String> {
        let s = match self {
            Value::Str(s) => s.clone(),
            Value::Expr(Expr::Literal(LiteralValue::String(s))) => s.to_string(),
            _ => panic!("not a string"),
        };
        Ok(s)
    }

    pub fn column(&self) -> FilterxResult<Column> {
        match self {
            Value::Column(c) => Ok(c.clone()),
            _ => Err(FilterxError::RuntimeError(
                "Only Column can convert to column".into(),
            )),
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

    pub fn is_column(&self) -> bool {
        match self {
            Value::Column(_) => true,
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
            Value::Expr(_) => true,
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
pub struct Column {
    pub col_name: String,
    pub new: bool,
    pub force: bool,
    pub data_type: Option<DataType>,
}

impl Column {
    pub fn new(col_name: String, new: bool) -> Self {
        Column {
            col_name,
            new,
            force: false,
            data_type: None,
        }
    }

    pub fn as_str(&self) -> &str {
        &self.col_name
    }
}

impl Default for Column {
    fn default() -> Self {
        Column {
            col_name: String::new(),
            new: false,
            force: false,
            data_type: None,
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct AttrMethod {
    pub col: Column,
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
            Value::Str(s) => s.clone(),
            Value::Expr(e) => e.to_string(),
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
            Value::AttrMethod(attr) => {
                let s = format!(
                    "AttrMethod({}.{} {:?})",
                    attr.col.col_name, attr.method, attr.value
                );
                s
            }
            Value::File(f) => f.file_name.clone(),
            Value::Column(c) => c.col_name.clone(),
            Value::Ident(i) => {
                let mut s = String::from("(");
                s.push_str(&i.0);
                s.push_str(": ");
                s.push_str(&i.1.to_string());
                s.push_str(")");
                s
            }
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
