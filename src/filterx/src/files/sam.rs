use crate::args::{SamCommand, ShareArgs};
use filterx_core::{util, writer::FilterxWriter, FilterxResult};
use filterx_engine::vm::Vm;
use filterx_source::{DataframeSource, Source, SourceType};
use polars::prelude::*;

fn init_sam_schema() -> Option<SchemaRef> {
    let mut files = Vec::<(String, DataType)>::new();
    files.push(("qname".into(), DataType::String));
    files.push(("flag".into(), DataType::UInt16));
    files.push(("rname".into(), DataType::String));
    files.push(("pos".into(), DataType::UInt32));
    files.push(("mapq".into(), DataType::UInt8));
    files.push(("cigar".into(), DataType::String));
    files.push(("rnext".into(), DataType::String));
    files.push(("pnext".into(), DataType::UInt32));
    files.push(("tlen".into(), DataType::Int32));
    files.push(("seq".into(), DataType::String));
    files.push(("qual".into(), DataType::String));
    util::create_schemas(files)
}

pub fn filterx_sam(cmd: SamCommand) -> FilterxResult<()> {
    let SamCommand {
        share_args:
            ShareArgs {
                input: path,
                expr,
                output,
                table,
                output_type,
            },
        header: include_header,
    } = cmd;

    let comment_prefix = "@";
    let separator = "\t";
    let writer = FilterxWriter::new(output.clone(), None, output_type)?;
    let schema = init_sam_schema();
    let lazy_df = util::init_df(
        path.as_str(),
        false,
        comment_prefix,
        separator,
        0,
        None,
        schema,
        None,
        true,
    )?;
    let names = vec![
        "qname", "flag", "rname", "pos", "mapq", "cigar", "rnext", "pnext", "tlen", "seq", "qual",
    ];
    let names = names.iter().map(|x| x.to_string()).collect::<Vec<String>>();
    let mut s = DataframeSource::new(lazy_df.clone());
    s.set_init_column_names(&names);
    let mut vm = Vm::from_source(Source::new(s.into(), SourceType::Sam), writer);
    let expr = util::merge_expr(expr);
    vm.eval_once(&expr)?;
    vm.finish()?;
    if vm.status.printed {
        return Ok(());
    }
    let mut df = vm.into_df()?;
    if output.is_none() && table.unwrap_or(false) {
        println!("{}", df);
        return Ok(());
    }
    if vm.status.printed {
        return Ok(());
    }
    let mut headers = None;
    if include_header.unwrap() {
        headers = Some(util::collect_comment_lines(path.as_str(), comment_prefix)?);
    }
    util::write_df(
        &mut df,
        &mut vm.writer,
        false,
        Some(separator),
        headers,
        Some("."),
    )
}
