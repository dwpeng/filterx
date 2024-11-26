# occ

filterx rows by occurence

`occ` is a function that filters rows by the number of times a value occurs in a column.

- `occ_lte`: less than or equal to the number of times a value occurs
- `occ_gte`: greater than or equal to the number of times a value occurs
- `occ`    : same as `occ_gte`

```csv title="data.csv"
a,b,c
1,2,3
2,2,1
2,2,3
2,1,3
```

```bash title="Example1"
filterx csv -H --oH data.csv 'occ(a, 2)'

# Output
a,b,c
2,2,1
2,2,3
2,1,3
```

`occ` supports multiple columns.

```bash title="Example2"
filterx csv -H --oH data.csv 'occ_lte(a, b, 1)'

# Output
a,b,c
2,2,1
2,2,3
```

## compute logics

groupby -> filter

