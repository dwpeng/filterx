use polars::prelude::DataType;
use polars::prelude::SchemaRef;

use super::args::{ShareArgs, VcfCommand};
use crate::engine::vm::Vm;
use crate::source::DataframeSource;

use crate::util;
use crate::FilterxResult;

fn init_vcf_schema(path: &str) -> FilterxResult<Option<SchemaRef>> {
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
    for filed in fields {
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
    Ok(util::create_schemas(schema))
}

pub fn filterx_vcf(cmd: VcfCommand) -> FilterxResult<()> {
    let VcfCommand {
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
    let schema = init_vcf_schema(&path)?;
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
    let headers = util::collect_comment_lines(path.as_str(), comment_prefix)?;
    util::write_df(
        &mut df,
        output.as_deref(),
        false,
        separator,
        Some(headers),
        Some("."),
    )
}
