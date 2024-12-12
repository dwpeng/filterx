use std::ops::Deref;

use polars::prelude::*;

use filterx_core::{FilterxResult, Hint};
use regex::Regex;

#[derive(Clone)]
pub struct DataframeSource {
    pub lazy: LazyFrame,
    pub has_header: bool,
    pub init_column_names: Vec<String>,
    pub ret_column_names: Vec<String>,
    pub is_in_and_ctx: bool,
}

pub fn detect_columns(df: LazyFrame) -> FilterxResult<Vec<String>> {
    let schema = df.lazy().collect_schema()?;
    Ok(schema.iter().map(|x| x.0.to_string()).collect())
}

impl DataframeSource {
    pub fn new(lazy: LazyFrame) -> Self {
        Self {
            lazy,
            has_header: true,
            init_column_names: vec![],
            ret_column_names: vec![],
            is_in_and_ctx: false,
        }
    }
}

impl DataframeSource {
    pub fn enter_and_ctx(&mut self) {
        self.is_in_and_ctx = true;
    }

    pub fn exit_and_ctx(&mut self) {
        self.is_in_and_ctx = false;
    }

    pub fn reset(&mut self) {
        self.has_header = true;
        self.init_column_names.clear();
        self.ret_column_names.clear();
    }

    pub fn set_init_column_names(&mut self, names: &Vec<String>) {
        self.init_column_names = names.clone();
        self.ret_column_names = names.clone();
    }

    pub fn set_has_header(&mut self, has_header: bool) {
        self.has_header = has_header;
    }

    pub fn index2column(&self, index: usize) -> String {
        if self.has_header {
            if index < self.ret_column_names.len() {
                return self.ret_column_names[index].clone();
            }
            let mut h = Hint::new();
            h.white("Have ")
                .cyan(&format!("{}", self.ret_column_names.len()))
                .white(" columns, but got index ")
                .cyan(&format!("{}", index))
                .print_and_exit();
        }
        format!("column_{}", index + 1)
    }

    pub fn set_index_with_name(&mut self, index: usize, name: &str) {
        let lazy = self.lazy.clone();
        let lazy = lazy.with_column(col(self.index2column(index)).alias(name));
        self.update(lazy);

        assert!(self.init_column_names.len() > index);
        self.ret_column_names[index] = name.to_string();
    }

    pub fn into_df(&self) -> FilterxResult<DataFrame> {
        let df = self.lazy.clone().collect()?;
        Ok(df)
    }

    pub fn lazy(&self) -> LazyFrame {
        self.lazy.clone()
    }

    pub fn update(&mut self, lazy: LazyFrame) {
        self.lazy = lazy
            .with_streaming(true)
            .with_slice_pushdown(true)
            .with_predicate_pushdown(true)
            .with_projection_pushdown(true)
            .with_simplify_expr(true)
            .with_collapse_joins(true)
            .with_type_coercion(true)
            .with_cluster_with_columns(true)
            .with_comm_subexpr_elim(true)
            .with_comm_subplan_elim(true)
            ._with_eager(true);
    }

    pub fn with_column(&mut self, e: Expr, new_name: Option<String>) {
        let lazy = self.lazy.clone();
        let lazy = lazy.with_column(e);
        self.update(lazy);

        if let Some(name) = new_name {
            self.ret_column_names.push(name);
        }
    }

    pub fn columns(&self) -> FilterxResult<Schema> {
        let schema = self.lazy.clone().collect_schema()?;
        let schema = schema.deref().clone();
        Ok(schema)
    }

    pub fn filter(&mut self, expr: Expr) {
        if self.is_in_and_ctx {
            return;
        }
        let lazy = self.lazy.clone();
        let lazy = lazy.filter(expr);
        self.update(lazy);
    }

    pub fn select(&mut self, columns: Vec<String>) {
        self.ret_column_names = columns.clone();
        let exprs = columns.iter().map(|x| col(x)).collect::<Vec<_>>();
        let lazy = self.lazy.clone();
        let lazy = lazy.select(exprs);
        self.update(lazy);
    }

    pub fn drop(&mut self, columns: Vec<String>) {
        for c in &columns {
            self.ret_column_names.retain(|x| x != c);
        }
        self.ret_column_names = self.ret_column_names.iter().map(|x| x.clone()).collect();

        let lazy = self.lazy.clone();
        let lazy = lazy.drop(columns);
        self.update(lazy);
    }

    pub fn unique(&mut self, columns: Vec<String>, keep: UniqueKeepStrategy) {
        let lazy = self.lazy.clone();
        let lazy = lazy.unique_generic(Some(columns), keep);
        self.update(lazy);
    }

    pub fn slice(&mut self, offset: i64, length: usize) {
        let lazy = self.lazy.clone();
        let lazy = lazy.slice(offset, length as IdxSize);
        self.update(lazy);
    }

    pub fn rename(&mut self, old: &str, new: &str) {
        if !self.check_column(old) {
            let mut h = Hint::new();
            h.white("Column ")
                .cyan(old)
                .white(" not found in the DataFrame")
                .print_and_exit();
        }
        if self.check_column(&new) {
            let mut h = Hint::new();
            h.white("Column ")
                .cyan(new)
                .white(" already exists in the DataFrame")
                .print_and_exit();
        }
        let idx = self.ret_column_names.iter().position(|x| x == old).unwrap();
        self.ret_column_names[idx] = new.to_string();

        let lazy = self.lazy.clone();
        let lazy = lazy.rename([old], [new], false);
        self.update(lazy);
    }

    pub fn has_column(&self, name: &str) -> () {
        let ret = self.check_column(name);
        if !ret {
            let re = Regex::new(name).unwrap();
            for c in &self.ret_column_names {
                if re.is_match(c) {
                    return;
                }
            }
            let mut h = Hint::new();
            h.white("Column ")
                .cyan(name)
                .white(" not found. Valid columns: ")
                .green(&self.ret_column_names.join(", "))
                .print_and_exit();
        }
        ()
    }

    pub fn check_column(&self, name: &str) -> bool {
        let i = self.ret_column_names.iter().find(|v| v.as_str() == name);
        i.is_some()
    }
}
