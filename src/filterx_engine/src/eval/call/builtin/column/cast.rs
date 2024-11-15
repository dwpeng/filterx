use super::super::*;
use polars::prelude::DataType;

pub fn cast<'a>(
    vm: &'a mut Vm,
    args: &Vec<ast::Expr>,
    new_type: &str,
    inplace: bool,
) -> FilterxResult<value::Value> {
    expect_args_len(args, 1)?;
    let col_name = eval_col!(
        vm,
        &args[0],
        "cast: expected a column name as first argument",
    );
    let name = col_name.column()?;
    let e = col_name.expr()?;
    vm.source_mut().has_column(name);
    let new_type = match new_type.to_lowercase().as_str() {
        "int" => DataType::Int32,
        "float" => DataType::Float32,
        "string" => DataType::String,
        "str" => DataType::String,
        "bool" => DataType::Boolean,
        "i32" => DataType::Int32,
        "i64" => DataType::Int64,
        "f32" => DataType::Float32,
        "f64" => DataType::Float64,
        "u32" => DataType::UInt32,
        "u64" => DataType::UInt64,
        "i8" => DataType::Int8,
        "i16" => DataType::Int16,
        "u8" => DataType::UInt8,
        "u16" => DataType::UInt16,
        "" => {
            let h = &mut vm.hint;
            h.white("cast: avaliable cast funtion: ")
                .green("cast_int")
                .white(", ")
                .green("cast_float")
                .white(", ")
                .green("cast_string")
                .white(", ")
                .green("cast_bool")
                .white(", ")
                .green("cast_i32")
                .white(", ")
                .green("cast_i64")
                .white(", ")
                .green("cast_f32")
                .white(", ")
                .green("cast_f64")
                .white(", ")
                .green("cast_u32")
                .white(", ")
                .green("cast_u64")
                .white(", ")
                .green("cast_i8")
                .white(", ")
                .green("cast_i16")
                .white(", ")
                .green("cast_u8")
                .white(", ")
                .green("cast_u16")
                .print_and_exit();
        }
        _ => panic!("unsupported type"),
    };

    let e = e.cast(new_type);
    if inplace {
        vm.source_mut().with_column(e.alias(name), None);
        return Ok(value::Value::None);
    }

    Ok(value::Value::named_expr(Some(name.to_string()), e))
}
