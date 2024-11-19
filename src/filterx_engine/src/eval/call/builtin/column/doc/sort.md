# sort

Sort the items in a column(s) in the table. There are another two functions `Sort` and `sorT` that are aliases of `sort`.

- `Sort`: from high to low
- `sorT`: from low to high
- `sort` same as `sorT`


```csv title="data.csv"
a,b,c
1,2,3
3,2,1
2,1,3
```

```bash title="Example1"
filterx csv -H --oH data.csv 'sort(a)'

# Output
a,b,c
1,2,3
2,1,3
3,2,1
```

```bash title="Example2"
filterx csv -H --oH data.csv 'Sort(a)'

# Output
a,b,c
3,2,1
2,1,3
1,2,3
```

`sort` supports multiple columns.


```csv title="data.csv"
a,b,c
1,2,3
3,2,1
3,1,2
2,1,3
```

```bash title="Example3"
filterx csv -H --oH data.csv 'sort(a, b)'

# Output
a,b,c
1,2,3
2,1,3
3,1,2
3,2,1
```
