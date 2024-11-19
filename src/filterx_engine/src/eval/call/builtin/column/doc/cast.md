# cast

Convert values in a column to another type. `cast` need be invoked with the `cast_type` and `column_name` as arguments.

```csv title="test.csv"
a,b,c
a,1,1.1
b,2,2.2
c,3,3.3
```

```bash title=Example1
filterx csv -H --oH test.csv -e 'cast_str_(b);alias(a) = a + b'

# Output
a,b,c,d
a,1,1.100,a1
b,2,2.200,b2
c,3,3.300,c3
```

