use super::super::*;

pub fn header(vm: &mut Vm) -> FilterxResult<value::Value> {
    let source = &vm.source_mut();
    let schema = source.columns()?;
    println!("index\tname\ttype");
    for (index, (name, t)) in schema.iter().enumerate() {
        println!("{}\t{}\t{}", index, name, t);
    }
    std::process::exit(0);
}
