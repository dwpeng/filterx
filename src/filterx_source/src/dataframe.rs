use polars::prelude::*;

use filterx_core::{FilterxResult, Hint};
use regex::Regex;

#[derive(Clone)]
pub struct DataframeSource {
    pub lazy: LazyFrame,
    pub has_header: bool,
    pub init_column_names: Vec<String>,
    pub ret_column_names: Vec<String>,
}

pub fn detect_columns(df: LazyFrame) -> FilterxResult<Vec<String>> {
    let df = df.fetch(100)?;
    let schema = df.get_column_names();
    Ok(schema.iter().map(|x| x.to_string()).collect())
}

impl DataframeSource {
    pub fn new(lazy: LazyFrame) -> Self {
        let lazy = lazy.with_streaming(true);
        Self {
            lazy,
            has_header: true,
            init_column_names: vec![],
            ret_column_names: vec![],
        }
    }
}

impl DataframeSource {
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
            if self.init_column_names.len() > index {
                return self.init_column_names[index].clone();
            }
            let mut h = Hint::new();
            h.white("Have ")
                .cyan(&format!("{}", self.ret_column_names.len()))
                .white(" columns, but got index ")
                .cyan(&format!("{}", index))
                .print_and_exit();
        }
        format!("column_{}", index)
    }

    pub fn set_index_with_name(&mut self, index: usize, name: &str) {
        let lazy = self.lazy.clone();
        let lazy = lazy.with_column(col(self.index2column(index)).alias(name));
        self.update(lazy);

        assert!(self.init_column_names.len() > index);
        self.ret_column_names[index] = name.to_string();
    }

    pub fn into_df(self) -> FilterxResult<DataFrame> {
        let df = self.lazy.collect()?;
        Ok(df)
    }

    pub fn lazy(&self) -> LazyFrame {
        self.lazy.clone()
    }

    pub fn update(&mut self, lazy: LazyFrame) {
        self.lazy = lazy
            .with_streaming(true)
            .with_simplify_expr(true)
            .with_collapse_joins(true);
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
        let df = self.lazy.clone().fetch(20)?;
        let schema = df.schema();
        Ok(schema)
    }

    pub fn finish(&mut self) -> FilterxResult<()> {
        let df = self.lazy.clone().collect()?;
        self.update(df.lazy());
        Ok(())
    }

    pub fn filter(&mut self, expr: Expr) {
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

    pub fn rename<I, J, T, S>(&mut self, columns: I, names: J)
    where
        I: IntoIterator<Item = T>,
        J: IntoIterator<Item = S>,
        T: AsRef<str>,
        S: AsRef<str>,
    {
        let old = columns
            .into_iter()
            .map(|x| x.as_ref().to_string())
            .collect::<Vec<String>>();
        let new = names
            .into_iter()
            .map(|x| x.as_ref().to_string())
            .collect::<Vec<String>>();

        for o in &old {
            if !self.ret_column_names.contains(o) {
                let mut h = Hint::new();
                h.white("Column ")
                    .cyan(o)
                    .white(" not found in the DataFrame")
                    .print_and_exit();
            }
            let idx = self.ret_column_names.iter().position(|x| x == o).unwrap();
            self.ret_column_names[idx] = new[idx].clone();
        }

        let lazy = self.lazy.clone();
        let lazy = lazy.rename(old, new, false);
        self.update(lazy);
    }

    pub fn has_column(&self, name: &str) -> () {
        let ret = self.ret_column_names.contains(&name.to_string());
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
        return self.ret_column_names.contains(&name.to_string());
    }
}
