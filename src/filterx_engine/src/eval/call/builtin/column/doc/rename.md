# rename

rename a column in the table with a new name

```csv title="data.csv"
name,age
alice,34
bob,23
```

```bash title="Example"
filterx csv -H --oH data.csv 'rename(name, first_name)'

# Output
first_name,age
alice,34
bob,23
```
