use crate::args::{ShareArgs, VcfCommand};
use filterx_core::{util, writer::FilterxWriter, FilterxResult};
use filterx_engine::vm::Vm;
use filterx_source::{DataframeSource, Source, SourceType};
use polars::prelude::*;

fn init_vcf_schema(path: &str) -> FilterxResult<(Vec<String>, Option<SchemaRef>)> {
    use std::fs::File;
    use std::io::BufRead;
    use std::io::BufReader;
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut line = String::new();
    loop {
        reader.read_line(&mut line)?;
        if line.starts_with("##") {
            line.clear();
            continue;
        }
        if line.starts_with("#") {
            line = line
                .strip_prefix("#")
                .unwrap()
                .to_string()
                .trim()
                .to_string();
            break;
        }
    }
    let mut schema = Vec::<(String, DataType)>::new();
    let fields: Vec<&str> = line.split("\t").collect();
    let mut names = vec![];
    for filed in fields {
        names.push(filed.to_ascii_lowercase());
        match filed {
            "CHROM" => schema.push(("chrom".into(), DataType::String)),
            "POS" => schema.push(("pos".into(), DataType::UInt32)),
            "ID" => schema.push(("id".into(), DataType::String)),
            "REF" => schema.push(("ref".into(), DataType::String)),
            "ALT" => schema.push(("alt".into(), DataType::String)),
            "QUAL" => schema.push(("qual".into(), DataType::Float32)),
            "FILTER" => schema.push(("filter".into(), DataType::String)),
            "INFO" => schema.push(("info".into(), DataType::String)),
            "FORMAT" => schema.push(("format".into(), DataType::String)),
            _ => {
                let field = filed.to_ascii_lowercase();
                schema.push((field, DataType::String));
            }
        }
    }
    Ok((names, util::create_schemas(schema)))
}

pub fn filterx_vcf(cmd: VcfCommand) -> FilterxResult<()> {
    let VcfCommand {
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
    let (names, schema) = init_vcf_schema(&path)?;
    let lazy_df = util::init_df(
        path.as_str(),
        false,
        comment_prefix,
        separator,
        0,
        None,
        schema,
        Some(vec!["."]),
        true,
    )?;
    let mut s = DataframeSource::new(lazy_df.clone());
    s.set_init_column_names(&names);
    let mut vm = Vm::from_source(Source::new(s.into(), SourceType::Vcf), writer);
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
