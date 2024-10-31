
## in

check if a value is in a set of values

```csv title="test.csv"
name,count
rust-1,1
rust-2,2
python-1,3
python-2,4
go-1,5
go-2,6
```

```bash
filterx csv -H --oH test.csv "name in ('rust-1', 'python-1')"

## Output
# name,count
# rust-1,1
# python-1,3
```

check if a substring is in a Column

```bash
filterx csv -H --oH test.csv "'rust' in name"

## Output
# name,count
# rust-1,1
# rust-2,2
```

## not in

same as `in` but opposite

