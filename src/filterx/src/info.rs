use crate::args::InfoArgs;
use filterx_core::FilterxResult;
use filterx_engine::eval::call::functions::get_function;
use filterx_info::render;

pub fn filterx_info(info: InfoArgs) -> FilterxResult<()> {
    let InfoArgs { name } = info;
    let f = get_function(&name);
    render::render_markdown_help(f.doc)?;
    Ok(())
}
