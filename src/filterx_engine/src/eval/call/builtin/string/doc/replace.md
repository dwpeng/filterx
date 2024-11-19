# replace

Replace substring with another pattern. There two kind of function: `replace` and `replace_one`. The difference between them is that `replace` will replace all occurrences of the substring, while `replace_one` will replace only the first occurrence.


```fa title="test.fa"
>seq1
AAGGAACC
```

```bash title="Example1"
filterx fa test.fa -e "replace_(seq, 'A', 'G')"

# output
>seq1
GGGGGGCC
```

```bash title="Example2"
filterx fa test.fa -e "replace_one(seq, 'A', 'G')"
# output
>seq1
GGGGAACC
```

There are also support for regular expression.

```csv title="test.csv"
a
apppppple
apple
```

```bash title="Example3"
filterx csv -H --oH test.csv -e "replace_(a, 'p{3,}', 'pp')"

# output
a
apple
apple
```
