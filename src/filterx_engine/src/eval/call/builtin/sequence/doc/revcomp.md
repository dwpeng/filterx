# revcomp

compute the reverse complement of a sequence

```fasta title="test.fa"
>seq1
ATGC
>seq2
ATGCC
```

```bash title="example"
filterx fa test.fa -e "revcomp_(seq)""

# output
>seq1
GCAT
>seq2
GGCAT
```
