# dup

Duplicate items in a column(s). There are 4 functions:

- `dup` - Duplicate items in a column and keep the first occurrence.
- `dup_last` - Duplicate items in a column and keep the last occurrence.
- `dup_any` - Duplicate items in a column and keep any one occurrence.
- `dup_none` - Duplicate items in a column and keep no occurrence.


```csv title="data.csv"
name,country
Alice,USA
Bob,UK
Alice,Canada
Alice,USA
```

```bash title=Example
filterx csv -H --oH data.csv 'dup(name)'

# Output
name,country
Alice,USA
Bob,UK
```

`dup` supports multiple columns.

```bash title=Example
filterx csv -H --oH data.csv 'dup(name, country)'
# Output
name,country
Alice,USA
Bob,UK
Alice,Canada
```


