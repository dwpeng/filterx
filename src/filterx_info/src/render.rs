use colored::Colorize;

use crate::parse::parse;

pub fn render_markdown_help(help: &str) {
    let lines = parse(help);
    for line in lines {
        print!("{}", line);
    }
    println!();
}

pub fn render_alias_function(alias_names: &[&'static str]) {
    if alias_names.len() == 1 {
        return;
    }
    print!("\n\nOther alias: ");
    for name in alias_names.iter().skip(1) {
        print!("{} ", name.green())
    }
    println!();
}
