use polars::prelude::*;

use crate::FilterxResult;

pub struct DataframeSource {
    pub lazy: LazyFrame,
    pub has_header: bool,
    pub n_cols: usize,
    pub n_rows: usize,
}

impl DataframeSource {
    pub fn new(lazy: LazyFrame) -> Self {
        let lazy = lazy.with_streaming(true);
        Self {
            lazy,
            has_header: true,
            n_cols: 0,
            n_rows: 0,
        }
    }
}

impl DataframeSource {
    pub fn set_has_header(&mut self, has_header: bool) {
        self.has_header = has_header;
    }

    pub fn index2column(index: usize) -> String {
        format!("column_{}", index)
    }

    pub fn set_index_with_name(&mut self, index: usize, name: &str) {
        let lazy = self.lazy.clone();
        let lazy = lazy.with_column(col(DataframeSource::index2column(index)).alias(name));
        self.update(lazy);
    }

    pub fn into_df(self) -> FilterxResult<DataFrame> {
        let df = self.lazy.collect()?;
        Ok(df)
    }

    pub fn lazy(&self) -> LazyFrame {
        self.lazy.clone()
    }

    pub fn update(&mut self, lazy: LazyFrame) {
        self.lazy = lazy.with_streaming(true);
    }

    pub fn with_column(&mut self, e: Expr) {
        let lazy = self.lazy.clone();
        let lazy = lazy.with_column(e);
        self.update(lazy);
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

    pub fn select(&mut self, exprs: Vec<Expr>) {
        let lazy = self.lazy.clone();
        let lazy = lazy.select(exprs);
        self.update(lazy);
    }

    pub fn drop(&mut self, columns: Vec<String>) {
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
        let lazy = self.lazy.clone();
        let lazy = lazy.rename(columns, names);
        self.update(lazy);
    }
}
