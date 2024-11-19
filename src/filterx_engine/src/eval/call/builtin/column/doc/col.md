# col

Select a column by `name` or `index`.

For a csv with header, you can directly use the column name to select the column.


```csv title="data.csv"
name,age
Alice,30
Bob,25
```



```bash title="Example1"
filterx csv data.csv -H --oH -e "select(col(name))"

# Output:
name
Alice
Bob
```

For a csv without header, you can use the column index to select the column. The index starts from 0.


```csv title="data.csv"
Alice,30
Bob,25
```

```bash title="Example2"
filterx csv data.csv --oH -e "select(col(0))"
# Output:
Alice
Bob
```

`col` can also be used to select multiple columns by using regex.

```csv title="data.csv"
a1,a2,a3,b
1,2,3,4
5,6,7,8
```

```bash title="Example3"
filterx csv data.csv --oH -e "select(col('^a\d+$'))"

# Output:
a1,a2,a3
1,2,3
5,6,7
```
