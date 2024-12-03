# drop_null

drop rows with null values


```txt title="test.csv"
name,age
Alice,20
Bob,
,30
```

```bash
filterx csv test.csv -H --oH -e 'drop_null(age)'

# output
name,age
Alice,20
,30
```

allow multiple columns

```bash
filterx csv test.csv -H --oH -e 'drop_null(age, name)'

# output
name,age
Alice,20
```

if no column is specified, it will apply to all columns, any row with null value will be dropped

```bash
filterx csv test.csv -H --oH -e 'drop_null()'

# output
name,age
Alice,20
```
