use colored::Colorize;
use filterx_core::FilterxResult;

use crate::parse::parse;

pub fn render_markdown_help(help: &str) -> FilterxResult<()> {
    let lines = parse(help);
    for line in lines {
        print!("{}", line);
    }
    println!();
    Ok(())
}

pub fn render_alias_function(alias_names: &[&'static str]) -> FilterxResult<()> {
    if alias_names.len() == 1 {
        return Ok(());
    }
    print!("\n\nOther alias: ");
    for name in alias_names.iter().skip(1) {
        print!("{} ", name.green())
    }
    println!();
    Ok(())
}
