# abs

Compute the absolute value of a number.

```csv title="test.csv"
a
-2
0
1
```

```bash title="Example1"
filterx csv -H --oH test.csv 'abs_(a)'

# Output
a
2
0
1
```

```bash title="Example2"
filterx csv -H --oH test.csv 'abs(a) > 1'

# Output
a
2
```
