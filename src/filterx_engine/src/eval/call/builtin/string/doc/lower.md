# lower

Converts a string to lowercase.

```fasta title="test.fasta"
>seq1
ACGT
>seq2
AGCTGGG
```

```bash title="Example1"
filterx fa test.fasta -e "print('{lower(seq)}')"

# output
acgt
agctggg
```

```bash title="Example2"
filterx fa test.fasta -e "alias(lower_seq) = lower(seq)" \
                      -e "print('{lower_seq}\t{seq}')"

# output
acgt	ACGT
agctggg	AGCTGGG
```
