# limit

Limit the number of rows returned by a query.

```csv title="test.csv"
a,b,c
1,2,3
4,5,6
7,8,9
```

```bash title="Example1"
filterx csv -H --oH test.csv -e "limit(2)"
# output
a,b,c
1,2,3
4,5,6
```

```bash title="Example2"
filterx csv -H --oH test.csv -e "b > 2;limit(1)"
# output
a,b,c
4,5,6
```

Similar to `head`, but works on `fastq` and `fasta` files as well.
