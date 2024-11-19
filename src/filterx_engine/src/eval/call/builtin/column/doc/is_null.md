# is_null

filters rows with NULL values or not

- `is_null` : filter rows with NULL values
- `is_not_null` : filter rows without NULL values


```txt title="test.csv"
name,age
Alice,20
Bob,
Charlie,30
```

```bash title=Example1
filterx csv test.csv -H --oH -e 'is_null(age)'

# output
name,age
Bob,
```

```bash title=Example2
filterx csv test.csv -H --oH -e 'is_not_null(age)'
# output
name,age
Alice,20
Charlie,30
```
