
```txt title="test.fa"
>seq1
ACGGGGT
>seq2
ACGTAAAAAA
```

## gc

computes the GC content of a sequence.

```bash
filterx fasta test.fa -e 'gc(seq) >= 0.5'
```

```txt

### Output
>seq1
ACGGGGT
```

## rev & rev_

`rev_` reverses a sequence in place, while `rev` will return a new reversed sequence.

reverses a sequence.

```bash
filterx fasta test.fa -e 'rev_(seq)'

### Output
>seq1
TGGGGCA
>seq2
AAAAAAAGCA
```

## revcomp & revcomp_

reverses and complements a sequence.

```bash
filterx fasta test.fa -e 'revcomp_(seq)'

### Output
>seq1
ACCCCGT
>seq2
TTTTTTACGT
```


## to_fasta & to_fa

converts a sequence to a FASTA format. Only available for fasta and fastq files.

```txt title="test.fa"
>seq1
ACGGGGT
>seq2
ACGTAAAAAA
aa
```

```bash
filterx fasta test.fa -e 'to_fasta()'

### Output
>seq1
ACGGGGT
>seq2
ACGTAAAAAAaa
```

```txt title="test.fq"
@name
ACGGGGT
+
~~~~~~~
@name2
ACGTAAAAAA
+
~~~~~~~~~~
```

```bash
filterx fastq test.fq -e 'to_fa()'

### Output
>name
ACGGGGT
>name2
ACGTAAAAAA
```

Not only `to_fasta/to_fa` can convert to FASTA, while using `filterx fq --no-qual test.fq` will remove the quality information, and auto-convert to FASTA.

```bash
filterx fq --no-qual test.fq

### Output
>name
ACGGGGT
>name2
ACGTAAAAAA
```


## to_fastq & to_fq

same as `to_fasta` but for fastq files. Only available for fasta and fastq files.
