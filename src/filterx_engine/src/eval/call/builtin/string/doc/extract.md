# extract

Extracts a substring from a string by using a regular expression.

```csv title="test.csv"
name
Alice-20
Bob-30
```

```bash
filterx csv test.csv -H -e 'alias(age) = extract(name, "(\d+)")'

# Output
name,age
Alice-20,20
Bob-30,30
```
