# print

Format and print the given value.

```csv title="data.csv"
name,age
alice,34
bob,23
```

```bash title="Example1"
filterx csv -H --oH data.csv "print('{name} is {age} years old')"

# Output
alice is 34 years old
bob is 23 years old
```

```fastq title="data.fastq"
@seq1
ACGT
+
IIII
@seq2
TGCA
+
IIII
```


```bash title="Example2"
filterx fq data.fastq "print('>{name}\n{seq}')"

# Output
>seq1
ACGT
>seq2
TGCA
```

`print` can also call functions in format strings.

```bash title="Example3"
filterx csv -H --oH data.fastq "print('{name}\t{gc(seq)}\t{len(seq)}')"

# Output
seq1	0.5	4
seq2	0.5	4
```
