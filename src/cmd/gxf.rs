use polars::prelude::DataType;
use polars::prelude::SchemaRef;

use super::args::{GFFCommand, ShareArgs};
use crate::engine::vm::Vm;
use crate::source::DataframeSource;

use crate::util;
use crate::FilterxResult;

fn init_gxf_schema() -> Option<SchemaRef> {
    let mut files = Vec::<(&'static str, DataType)>::new();
    files.push(("seqid", DataType::String));
    files.push(("source", DataType::String));
    files.push(("type", DataType::String));
    files.push(("start", DataType::UInt32));
    files.push(("end", DataType::UInt32));
    files.push(("score", DataType::Float32));
    files.push(("strand", DataType::String));
    files.push(("phase", DataType::UInt8));
    files.push(("attributes", DataType::String));
    util::create_schemas(files)
}

pub fn filterx_gxf(cmd: GFFCommand) -> FilterxResult<()> {
    let GFFCommand {
        share_args:
            ShareArgs {
                input: path,
                expr,
                output,
                table,
            },
    } = cmd;

    let comment_prefix = "#";
    let separator = "\t";
    let writer = util::create_buffer_writer(output.clone())?;
    let lazy_df = util::init_df(
        path.as_str(),
        false,
        comment_prefix,
        separator,
        0,
        None,
        init_gxf_schema(),
    )?;
    let mut s = DataframeSource::new(lazy_df.clone());
    s.set_has_header(false);
    let mut vm = Vm::from_dataframe(s);
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
    util::write_df(&mut df, output.as_deref(), false, separator)
}
