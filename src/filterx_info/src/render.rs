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
