use filterx_core::Hint;

use super::builtin::*;

pub struct BuiltinFunction {
    pub name: &'static str,
    pub alias: &'static [&'static str],
    pub can_expression: bool,
    pub can_inplace: bool,
    pub doc: &'static str,
}

pub static ALL_FUNCTIONS: [&'static [BuiltinFunction]; 5] = [
    FUNCTION_COLUMN,
    FUNCTION_STRING,
    FUNCTION_SEQUENCE,
    FUNCTION_NUMBER,
    FUNCTION_ROW,
];

fn compute_similarity(target: &str) -> Option<&str> {
    let mut best_score = 0.0;
    let mut best_name = "";
    for function_list in ALL_FUNCTIONS.iter() {
        for function in function_list.iter() {
            for alias in function.alias.iter() {
                let score = strsim::jaro_winkler(target, alias);
                if score > best_score {
                    best_score = score;
                    best_name = alias;
                }
            }
        }
    }

    if best_score >= 0.6 {
        return Some(best_name);
    }
    None
}

pub fn get_function(name: &str) -> &'static BuiltinFunction {
    let inplace = name.ends_with("_");
    let pure_name = if inplace {
        &name[..name.len() - 1]
    } else {
        name
    };
    for functions in ALL_FUNCTIONS.iter() {
        for function in functions.iter() {
            if function.alias.contains(&pure_name) {
                if inplace && !function.can_inplace {
                    let mut h = Hint::new();
                    h.white("Function: ")
                        .cyan(pure_name)
                        .bold()
                        .white(" can't be used as inplace.")
                        .print_and_exit();
                }
                return function;
            }
        }
    }

    let mut h = Hint::new();
    let simi = compute_similarity(&pure_name);
    h.white("Function `").cyan(&name).white("` does not found.");

    if simi.is_some() {
        h.white(" Similar function `")
            .green(simi.unwrap())
            .white("` found.");
    }
    h.print_and_exit();
}

pub fn list_functions() -> Vec<&'static BuiltinFunction> {
    let mut funcions = vec![];
    for function_group in ALL_FUNCTIONS.iter() {
        for function in function_group.iter() {
            funcions.push(function);
        }
    }
    funcions
}
