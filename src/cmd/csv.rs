use super::args::{CsvCommand, ShareArgs};
use crate::engine::vm::Vm;
use crate::source::DataframeSource;

use crate::util;
use crate::FilterxResult;

pub fn filterx_csv(cmd: CsvCommand) -> FilterxResult<()> {
    let CsvCommand {
        share_args:
            ShareArgs {
                input: path,
                expr,
                output,
                table,
            },
        header,
        output_header,
        comment_prefix,
        separator,
        output_separator,
        skip_row,
        limit_row,
    } = cmd;

    let comment_prefix = comment_prefix.unwrap();
    let separator = separator.unwrap();
    let writer = util::create_buffer_writer(output.clone())?;
    let lazy_df = util::init_df(
        path.as_str(),
        header.unwrap(),
        &comment_prefix,
        &separator,
        skip_row.unwrap(),
        limit_row,
        None,
        None,
        false,
    )?;
    let mut s = DataframeSource::new(lazy_df.clone());
    s.set_has_header(header.unwrap());
    let mut vm = Vm::from_dataframe(s);
    if header.is_some() {
        let lazy = util::init_df(
            path.as_str(),
            header.unwrap(),
            &comment_prefix,
            &separator,
            skip_row.unwrap(),
            limit_row,
            None,
            None,
            false,
        )?;
        vm.status.inject_columns_by_df(lazy)?;
    }
    let expr = util::merge_expr(expr);
    let writer = Box::new(writer);
    vm.set_writer(writer);
    vm.eval_once(&expr)?;
    vm.finish()?;
    if vm.status.printed {
        return Ok(());
    }
    let mut df = vm.source.into_dataframe().unwrap().into_df()?;
    if output.is_none() && table.unwrap_or(false) {
        println!("{}", df);
        return Ok(());
    }
    if vm.status.printed {
        return Ok(());
    }
    util::write_df(
        &mut df,
        output.as_deref(),
        output_header.unwrap(),
        output_separator.unwrap().as_str(),
        None,
        None,
    )
}
