use polars::prelude::*;

use crate::error::FilterxResult;

pub trait TableLike<'a> {
    type Table: IntoIterator<Item = Self::Record>;
    type Record: std::fmt::Display;
    type ParserOptions;

    fn from_path(path: &str) -> FilterxResult<Self::Table>;
    fn into_dataframe(self) -> FilterxResult<DataFrame>;
    fn as_dataframe(
        records: Vec<Self::Record>,
        parser_options: &Self::ParserOptions,
    ) -> FilterxResult<DataFrame>;
    fn parse_next(&'a mut self) -> FilterxResult<Option<&'a mut Self::Record>>;
    fn reset(&mut self) -> FilterxResult<()>;
    fn set_parser_options(self, parser_options: Self::ParserOptions) -> Self;
}
