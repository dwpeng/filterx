```csv title="test.csv"
name,age
Alice-Alice,30
Bob,25
Charlie,35
```


## lower & lower_

lowercase the string and `lower_` will lowercase the string in place.

```shell
filterx csv test.csv -e -H --oH 'lower_(name)'

### Output
name,age
alice-alice,30
bob,25
charlie,35
```


## upper & upper_

same as `lower` but for uppercase.

## replace & replace_

replace will replace all occurrences of a substring with another substring, while `replace_` will replace in place.

```shell
filterx csv test.csv -e -H --oH 'replace(name, "Alice", "lady")'

### Output
name,age
lady-lady,30
Bob,25
Charlie,35
```

## replace_one & replace_one_

same as `replace` but only replaces the first occurrence.

```shell
filterx csv test.csv -e -H --oH 'replace_one(name, "Alice", "lady")'

### Output
name,age
lady-Alice,30
Bob,25
Charlie,35
```

## slice & slice_

get a slice of the string, `slice` will return a new string while `slice_` will modify the string in place.

`slice` assumes 0-based indexing, so `slice(name, 3)` will return string from 0 with length 3. `slice(name, 3, 2)` will return string from 3 with length 2.

```shell
filterx csv test.csv -e -H --oH 'slice(name, 3)'
### Output
name,age
Ali,30
Bob,25
Cha,35
```
it also supports `(start, length)` syntax.

```shell
filterx csv test.csv -e -H --oH 'slice(name, 3, 2)'
### Output
name,age
ce,30
ob,25
rl,35
```

## strip & strip_

remove leading and trailing string, `strip` will return a new string while `strip_` will modify the string in place.

```csv title="strip.csv"
a
applepleap
oommmoo
```

```shell
filterx csv strip.csv -e -H --oH 'strip_(a, "ap")'

### Output
a
pleple
oommmoo
```

## lstrip & lstrip_

same as `strip` but only remove leading string.

## rstrip & rstrip_

same as `strip` but only remove trailing string.

## len

returns the length of the string.

```shell
filterx csv test.csv -e -H --oH 'alias(len) = len(name)'

### Output
name,age,len
Alice-Alice,30,11
Bob,25,3
Charlie,35,7
```
