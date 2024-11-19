use crate::args::InfoArgs;
use filterx_core::{hint::Colorize, FilterxResult, Hint};
use filterx_engine::eval::call::functions::{get_function, list_functions};
use filterx_info::render;

fn list_and_print() -> FilterxResult<()> {
    let functions = list_functions();
    for (index, func) in functions.iter().enumerate() {
        println!("{:2} {}", (index + 1), func.name.cyan().bold());
    }
    println!();
    let mut h = Hint::new();
    h.white("Use ")
        .green("filterx info <function_name>")
        .bold()
        .white(" to get more information about a function.")
        .print_and_clear();
    Ok(())
}

pub fn filterx_info(info: InfoArgs) -> FilterxResult<()> {
    let InfoArgs { name, list } = info;

    let list = list.unwrap();

    if !list && name.is_none() {
        println!("filterx info {}", "<name>".cyan());
        return Ok(());
    }

    if list {
        list_and_print()?;
        return Ok(());
    }

    let f = get_function(&name.unwrap());
    render::render_markdown_help(f.doc)?;
    render::render_alias_function(&f.alias)?;
    Ok(())
}
