
```csv title="test.csv
name,age
Alice,20
Bob,30
Charlie,40
```

```csv title="test-without-header.csv
Alice,20
Bob,30
Charlie,40
```

## col

get column by name or index, for index, it starts from 1.

```bash
filterx csv test.csv -H -e 'col(name) == "Alice"'
# equivalent to
filterx csv test-without-header.csv -e 'col(1) == "Alice"'

# output
# Alice,20
```

`col` can be used as column name, so you can use it in any need of column name. And it also can be used in `col` function.

```bash
filterx csv test.csv -H -e 'col(col(1)) == "Alice"'
```

::: warning
while using index, the csv file should not have header.

```bash
filterx csv test.csv -H -e 'col(1) == "Alice"' # error
```
:::


## select

get columns by name, output will follow the order of selection.

```bash
filterx csv test.csv -H -e 'select(age, name)'

# output
# 20,Alice
# 30,Bob
# 40,Charlie
```

## alias

create a new column from a literal value or an expression or a column.

```bash
filterx csv test.csv -H --oH -e 'alias(new_col) = 10'

# output
# name,age,new_col
# Alice,20,10
# Bob,30,10
# Charlie,40,10
```

```bash
filterx csv test.csv -H --oH -e 'alias(new_col) = age + 10'

# output
# name,age,new_col
# Alice,20,30
# Bob,30,40
# Charlie,40,50
```

::: tip why not create a new column directly
`alias` is not a function, it is a statement. I will know where the new column is created, and it is more readable.
:::


## drop

drop columns by name. It is more like `select` but if you don't want some columns, use `drop` will be more memory efficient.

```bash
filterx csv test.csv -H --oH -e 'drop(age)'
# output
# name
# Alice
# Bob
# Charlie
```

## rename

rename columns by name.

```bash
filterx csv test.csv -H --oH -e 'rename(name, new_name)'
# output
# new_name,age
# Alice,20
# Bob,30
# Charlie,40
```

## sort

sort by column(s), it can sort by multiple columns. This function provide three ways to sort.

The default way (`sort`) is from low to high, and you can use `Sort` to sort from high to low, and `sorT` to sort from low to high.


```bash

filterx csv test.csv -H --oH -e 'sort(age)'
# output
# name,age
# Alice,20
# Bob,30
# Charlie,40


filterx csv test.csv -H --oH -e 'Sort(age)'
# output
# name,age
# Charlie,40
# Bob,30
# Alice,20
```

sort support multiple columns.

```txt title="multi-sort.csv"
a,b,c
1,2,3
1,1,3
1,1,2
```

```bash
filterx csv multi-sort.csv -H --oH -e 'sort(a, b)'
# output
# a,b,c
# 1,1,2
# 1,1,3
# 1,2,3
```


## header

print headers of files. so that you can know the column names to manipulate them.

```txt title="test.vcf
##fileformat=VCFv4.2
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##reference=file://some/path/human_g1k_v37.fasta
##INFO=<ID=END,Number=1,Type=Integer,Description="End position of the variant described in this record">
##INFO=<ID=MinDP,Number=1,Type=Integer,Description="Dummy">
##ALT=<ID=DEL,Description="Deletion">
##contig=<ID=1,assembly=b37,length=249250621>
##contig=<ID=2,assembly=b37,length=249250621>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	NA001
1	5	.	C	a	.	PASS	.	GT	0/1
1	5	.	C	t	.	PASS	.	GT	0/1
1	7	.	T	a	.	PASS	.	GT	.
1	10	.	G	a	.	PASS	.	GT	0/1
1	12	.	GACA	ga	.	PASS	.	GT	0/1
1	16	.	T	taaa	.	PASS	.	GT	1/1
1	19	.	A	c	.	PASS	.	GT	0/1
1	61	.	C	a	.	PASS	.	GT	0/1
2	61	.	AGAG	aa	.	PASS	.	GT	0/1
2	119	.	AAA	t	.	PASS	.	GT	0/1
2	179	.	G	gacgtacgt	.	PASS	.	GT	0/1
2	200	.	A	<DEL>	.	PASS	END=210	GT	1/0
2	300	.	A	.	.	PASS	END=310;MinDP=10	GT	0/1
2	320	.	A	<*>	.	PASS	END=330;MinDP=20	GT	0/1
2	481	.	T	c,a	.	PASS	.	GT	0/2
```

```bash
filterx vcf test.vcf -e 'header()'

# output
chrom   str
pos     u32
id      str
ref     str
alt     str
qual    f32
filter  str
info    str
format  str
na001   str
```