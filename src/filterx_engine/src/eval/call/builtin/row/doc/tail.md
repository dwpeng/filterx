# tail

Get the last `n` rows of a files.

```csv title="test.csv"
a,b,c
1,2,3
4,5,6
7,8,9
```

```bash title="Example1"
filterx csv -H --oH test.csv -e "tail(2)"

# output
a,b,c
4,5,6
7,8,9
```

```bash title="Example2"
filterx csv -H --oH test.csv -e "b > 2;tail(1)"

# output
a,b,c
7,8,9
```
