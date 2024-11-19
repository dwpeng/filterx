# phred

Check the quality of a sequence using the Phred algorithm.

There are two kinds of Phred scores: phred33 and phred64.

```fastq title="test.fq"
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
filterx fq test.fq -e "phred()"

# output
phred: phred64
```
