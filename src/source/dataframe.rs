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
}
