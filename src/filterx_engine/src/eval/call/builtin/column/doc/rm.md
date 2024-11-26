# rm

Delete a column(s) from a table.

```csv title="data.csv"
a1,a2,a3,b
1,2,3,4
5,6,7,8
```

```bash title=Example
filterx csv data.csv -e "rm(a1)"

# Output
a2,a3,b
2,3,4
6,7,8
```

