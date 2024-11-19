# header

print the table headers and exit

```csv title="data.csv"
a1,a2,a3,b
1,2,3,4
5,6,7,8
```

```bash title=Example
filterx csv data.csv -e "header()"

# Output
index   name    type
0       a1      i64
1       a2      i64
2       a3      i64
3       b       i64
```
