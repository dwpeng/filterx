# revcomp

compute the HPC of a sequence, compress contiguous identical characters into a single character

```fasta title="test.fa"
>seq1
ATGCCCCCT
```

```bash title="example"
filterx fa test.fa -e "hpc_(seq)""

# output
>seq1
ATGCT
```
