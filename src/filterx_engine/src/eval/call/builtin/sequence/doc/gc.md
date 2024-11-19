# gc

Compute the gc content of a sequence

```fasta title="test.fa"
>seq1
ATGC
>seq2
ATGCC
```

```bash title="example"
filterx fasta test.fa -e "gc(seq) > 0.5"

# output
>seq2
ATGCC
```
