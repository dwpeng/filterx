# qual

Compute the mean quality of a sequence.

```fasta title="test.fq"
@seq1
ATGC
+
IIII
@seq2
ATGCC
+
IIIII
```

```bash title="example"
filterx fq test.fq -e "print('{name}: {qual(seq)}')"
```

```bash title="output"

# output
seq1: 4.245115
seq2: 3.9658952
```
