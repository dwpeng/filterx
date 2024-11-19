# to_fasta

Convert a fastq file to fasta format.

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
filterx fq test.fq -e "to_fasta()"

# output
>seq1
ATGC
>seq2
ATGCC
```

For a `fastq` file, you can directly use `--no-qual` option to convert it to `fasta` format.

```bash title="example"
filterx fq test.fq --no-qual

# output
>seq1
ATGC
>seq2
ATGCC
```

