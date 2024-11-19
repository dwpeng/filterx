# select

Selects the columns of a table to output. The columns are specified by their names.

```csv title="data.csv"
name,age
alice,34
bob,23
```

```bash title="Example1"
filterx csv -H --oH data.csv 'select(name)'
# Output

name
alice
bob
```

```bash title="Example2"
filterx csv -H --oH data.csv 'select(age, name)'

# Output
age,name
34,alice
23,bob
```
