# slice

Extract a substring from a string.

```fasta title="test.fa"
>seq1
ACGTCTGATGCATCTAGTCTACAG
```

```bash title="Example1"
# get the first 5 characters
filterx fa test.fa -e "slice_(seq, 5)"

# output
>seq1
ACGTC
```

```bash title="Example2"
# get sub sequence start from 1 with length 5
filterx fa test.fa -e "slice_(seq, 1, 5)" # offset starts from 0, so 1 is the second character

# output
>seq1
ACGTC
```
