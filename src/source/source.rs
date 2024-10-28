use polars::prelude::*;

use super::DataframeSource;
use crate::FilterxResult;
pub enum Source {
    Dataframe(DataframeSource),
}

impl Source {
    pub fn new_dataframe(dataframe: DataframeSource) -> Self {
        Source::Dataframe(dataframe)
    }
}

impl Source {
    pub fn finish(&mut self) -> FilterxResult<()> {
        match self {
            Source::Dataframe(df) => {
                let plan = df.lazy.describe_optimized_plan();
                if plan.is_err() {
                    return Ok(());
                }
                let plan = plan.unwrap();
                if plan.len() == 0 {
                    return Ok(());
                }
                let ret_df = df.lazy.clone().collect()?;
                df.update(ret_df.lazy());
            }
        };
        Ok(())
    }

    pub fn dataframe_mut_ref(&mut self) -> Option<&mut DataframeSource> {
        match self {
            Source::Dataframe(df) => Some(df),
        }
    }

    pub fn into_dataframe(self) -> Option<DataframeSource> {
        match self {
            Source::Dataframe(df) => Some(df),
        }
    }
}
