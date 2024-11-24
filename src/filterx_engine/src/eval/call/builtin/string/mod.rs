use crate::builtin_function;

builtin_function! {
    FUNCTION_STRING,
    (len, true, false),
    (upper, true, true),
    (lower, true, true),
    (slice, true, true),
    (replace, true, true, (replace_one)),
    (strip, true, true, (lstrip, rstrip)),
    (rev, true, true),
    (width, true, true, (w)),
    (trim, true, true),
}
