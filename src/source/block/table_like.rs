use polars::prelude::*;

use crate::error::FilterxResult;
use crate::source::block::record::Filter;

pub trait TableLike<'a> {
    type Table: IntoIterator<Item = Self::Record>;
    type Record: std::fmt::Display + Filter;
    type FilterOptions;
    type ParserOptions;

    fn from_path(path: &str) -> FilterxResult<Self::Table>;
    fn into_dataframe(self) -> FilterxResult<DataFrame>;
    fn as_dataframe(records: Vec<Self::Record>) -> FilterxResult<DataFrame>;
    fn parse_next(&'a mut self) -> FilterxResult<Option<&'a Self::Record>>;
    fn filter_next(&'a mut self) -> FilterxResult<Option<&'a Self::Record>>;
    fn reset(&mut self);
    fn set_filter_options(self, filter_options: Self::FilterOptions) -> Self;
    fn set_parser_options(self, parser_options: Self::ParserOptions) -> Self;
    fn columns(&self) -> &Vec<String>;
}
