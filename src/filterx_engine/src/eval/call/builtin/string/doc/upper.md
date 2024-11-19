# upper

Converts a string to upeercase.

```fasta title="test.fasta"
>seq1
acgt
>seq2
agctggg
```

```bash title="Example1"
filterx fa test.fasta -e "print('{upper(seq)}')"

# output
acgt
agctggg
```

```bash title="Example2"
filterx fa test.fasta -e "alias(upper_seq) = upper(seq)" \
                      -e "print('{upper_seq}\t{seq}')"

# output
ACGT	acgt
AGCTGGG	agctggg
```
