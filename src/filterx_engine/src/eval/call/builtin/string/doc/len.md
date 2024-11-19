# len

Compute the length of a string.

```fasta title="test.fasta"
>seq1
ACGT
>seq2
AGCTGGG
```

```bash title="Example1"
filterx fa test.fasta -e "print('{name}\t{len(seq)}')

# output
seq1	4
seq2	7
```
