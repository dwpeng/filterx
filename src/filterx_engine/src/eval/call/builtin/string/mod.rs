use crate::builtin_function;

builtin_function! {
    FUNCTION_STRING,
    (len, true),
    (upper, true),
    (lower, true),
    (slice, true),
    (replace, true),
    (strip, true, (lstrip, rstrip)),
    (rev, true),
}
