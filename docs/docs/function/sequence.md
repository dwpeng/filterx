
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
