# drop_null

drop rows with null values


```txt title="test.csv"
name,age
Alice,20
Bob,
Charlie,30
```

```bash
filterx csv test.csv -H --oH -e 'drop_null(age)'

# output
name,age
Alice,20
Charlie,30
```

