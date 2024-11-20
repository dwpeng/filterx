# width

Reformats a string to a given width. `width` chars per line.

```fa title="test.fa"
>aaa comment1
ctatgctatctatcatc
>bbb comment2
aaabbbCCC
```

```bash title="Example"
filterx fa test.fa -e "width_(seq, 3)"

# Output:
>aaa comment1
cta
tgc
tat
cta
tca
tc
>bbb comment2
aaa
bbb
CCC
```
