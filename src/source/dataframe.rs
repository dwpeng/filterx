use polars::prelude::*;

pub struct DataframeSource {
    pub df: Option<DataFrame>,
    pub lazy: LazyFrame,
}

impl DataframeSource {
    pub fn new(df: LazyFrame) -> Self {
        Self { df: None, lazy: df }
    }
}
