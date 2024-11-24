# trim

trim removes leading and trailing whitespace from a string.


```fasta title="test.fa"
>seq1
ACGTCTGATGCATCTAGTCTACAG
```

```bash title="Example1"
# trim 5bp from start and 3bp from end
filterx fa test.fa -e "trim_(seq, 5, 3)"

# output
>seq1
TGATGCATCTAGTCTA
```

