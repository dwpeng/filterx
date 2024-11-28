use crate::args::{GFFCommand, ShareArgs};
use filterx_core::{util, writer::FilterxWriter, FilterxResult};
use filterx_engine::vm::Vm;
use filterx_source::{DataframeSource, Source, SourceType};
use polars::prelude::*;

fn init_gxf_schema() -> Option<SchemaRef> {
    let mut files = Vec::<(String, DataType)>::new();
    files.push(("seqid".into(), DataType::String));
    files.push(("source".into(), DataType::String));
    files.push(("type".into(), DataType::String));
    files.push(("start".into(), DataType::UInt32));
    files.push(("end".into(), DataType::UInt32));
    files.push(("score".into(), DataType::Float32));
    files.push(("strand".into(), DataType::String));
    files.push(("phase".into(), DataType::UInt8));
    files.push(("attr".into(), DataType::String));
    util::create_schemas(files)
}

#[derive(Debug, Clone, Copy, PartialEq, clap::ValueEnum)]
pub enum GxfType {
    Gff,
    Gtf,
}

impl From<GxfType> for SourceType {
    fn from(g: GxfType) -> Self {
        match g {
            GxfType::Gff => SourceType::Gff,
            GxfType::Gtf => SourceType::Gtf,
        }
    }
}

pub fn filterx_gxf(cmd: GFFCommand, gxf_type: GxfType) -> FilterxResult<()> {
    let GFFCommand {
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
    let comment_prefix = "#";
    let separator = "\t";
    let writer = FilterxWriter::new(output.clone(), None, output_type)?;
    let schema = init_gxf_schema();
    let names = vec![
        "seqid", "source", "type", "start", "end", "score", "strand", "phase", "attr",
    ];
    let names = names.iter().map(|x| x.to_string()).collect::<Vec<String>>();
    let lazy_df = util::init_df(
        path.as_str(),
        false,
        comment_prefix,
        separator,
        0,
        None,
        schema,
        Some(vec![".", "?"]),
        true,
    )?;
    let mut s = DataframeSource::new(lazy_df.clone());
    s.set_init_column_names(&names);
    let mut vm = Vm::from_source(Source::new(s.into(), gxf_type.into()), writer);
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
