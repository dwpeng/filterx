# to_fastq

Convert a FASTA file to a FASTQ file. Quality scores will be set to `?`

```fasta title="test.fa"
>aaa comment1
ctatgctatctatcatc
>bbb comment2
aaa
bbb
CCC
```

```bash title="example"
filterx fa test.fa -e "to_fq()"

# output
@aaa
ctatgctatctatcatc
+
?????????????????
@bbb
aaabbbCCC
+
?????????
```
