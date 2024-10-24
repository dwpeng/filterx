use super::super::ast;
use super::super::value;

use crate::engine::eval::Eval;
use crate::engine::value::Value;
use crate::engine::vm::Vm;
use crate::eval;
use crate::FilterxError;
use crate::FilterxResult;

use polars::prelude::*;

impl<'a> Eval<'a> for ast::ExprConstant {
    type Output = value::Value;
    fn eval(&self, _vm: &'a mut Vm) -> FilterxResult<Self::Output> {
        let r = match &self.value {
            ast::Constant::Int(i) => {
                let v: Value = i.clone().into();
                v
            }
            ast::Constant::Float(f) => value::Value::Float(f.clone()),
            ast::Constant::Str(s) => value::Value::Str(s.clone()),
            _ => {
                return Err(FilterxError::RuntimeError(
                    "Unsupported type. Only int/float/str supported.".to_string(),
                ))
            }
        };
        Ok(r)
    }
}

impl<'a> Eval<'a> for ast::ExprTuple {
    type Output = value::Value;
    fn eval(&self, vm: &'a mut Vm) -> FilterxResult<Self::Output> {
        let mut r = Vec::new();
        for e in &self.elts {
            let v = eval!(vm, e, "Only support constant", Constant, Call);
            r.push(v);
        }
        Ok(value::Value::List(r))
    }
}

impl<'a> Eval<'a> for ast::ExprName {
    type Output = value::Value;
    fn eval(&self, vm: &'a mut Vm) -> FilterxResult<Self::Output> {
        let id = self.id.as_str().to_string();
        for col in &vm.status.selected_columns {
            if col.name == id {
                return Ok(value::Value::Column({
                    value::Column {
                        col_name: id,
                        new: col.new,
                        force: false,
                        data_type: Some(col.data_type.clone()),
                    }
                }));
            }
        }

        Ok(value::Value::Column({
            value::Column {
                col_name: id,
                new: true,
                force: false,
                data_type: Some(DataType::String),
            }
        }))
    }
}

// impl<'a> Eval<'a> for ast::ExprSlice {
//     type Output = value::Value;
//     fn eval(&self, vm: &'a mut Vm) -> FilterxResult<Self::Output> {
//         let mut slice = value::Slice::default();
//         match self.lower {
//             Some(ref l) => match l.deref() {
//                 ast::Expr::Constant(c) => {
//                     let v = c.eval(vm)?;
//                     slice.start = Some(Box::new(v));
//                 }
//                 _ => panic!("unsupported type"),
//             },
//             None => {}
//         }
//         match self.upper {
//             Some(ref u) => match u.deref() {
//                 ast::Expr::Constant(c) => {
//                     let v = c.eval(vm)?;
//                     slice.end = Some(Box::new(v));
//                 }
//                 _ => panic!("unsupported type"),
//             },
//             None => {}
//         }
//         Ok(value::Value::Slice(slice))
//     }
// }

impl<'a> Eval<'a> for ast::ExprAttribute {
    type Output = value::Value;
    fn eval(&self, _vm: &'a mut Vm) -> FilterxResult<Self::Output> {
        // TODO implement attribute
        Ok(value::Value::None)
    }
}
