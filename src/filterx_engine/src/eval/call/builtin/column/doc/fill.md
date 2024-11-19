# fill

fill value if the value is null or na. There are another two functions `fill_null` and `fill_na` which are aliases of `fill`.

- `fill_null`: fill value if the value is null.
- `fill_na`: fill value if the value is na.
- `fill`: same as `fill_null`.

```txt title="test.csv"
name,age
Alice,20
Bob,
Charlie,30
```

```bash
filterx csv test.csv -H --oH -e 'fill_null_(age, 0)'

# output
name,age
Alice,20
Bob,0
Charlie,30
```
