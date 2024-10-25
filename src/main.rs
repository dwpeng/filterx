use filterx::cmd;
fn main() {
    match cmd::cli() {
        Ok(_) => {}
        Err(e) => {
            eprintln!("{}", e);
            std::process::exit(1);
        }
    }
}
