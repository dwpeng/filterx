# strip

Remove prefix/suffix pattern from string. There are another two functions that can be used to remove only the prefix or suffix: `lstrip` and `rstrip`.

```fasta title="test.fa"
>seq1
AAGTCGAA
```

```bash title="Example1"
filterx fa test.fa -e "strip_(seq, 'A')"

# output
>seq1
GTCG
```

```bash title="Example2"
filterx fa test.fa -e "lstrip_(seq, 'A')"
# output
>seq1
GTCGAA
```

```bash title="Example3"
filterx fa test.fa -e "rstrip_(seq, 'A')"
# output
>seq1
AAGTCG
```
