# rev

Gets the reverse of a string.

```fasta title="test.fa"
>seq1
ATCG
```

```bash title="Example"
filterx fa test.fa -e "rev_(seq)"

# output
>seq1
GCTA
```
