# alias

While add a new column to the table, the new column name must be wrapped in `alias()` function. `alias` need a column name as argument, and only can be used in create column statement. 


```csv title=test.csv
a,b
1,2
3,4
```

create a new column named `c` with value `a + 1`:

```bash title="example"
filterx c -H test.csv -e "alias(c) = a + 1"

# output
a,b,c
1,2,2
3,4,4
```
