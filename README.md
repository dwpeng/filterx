
## filterx

A simple and lightweight tool for filtering csv file. It provides a simple way to filter csv file by column name and value. It is useful for filtering large csv files expescially when you need multiple filters. It is written in Rust and compiled to a single binary file. It is fast and efficient.


## Installation

### Cargo
```bash
cargo install --git https://github.com/dwpeng/filterx
```

### Pre-compiled binary
There is no need to install Rust toolchain if you just want to use the tool. The pre-compiled binary is available for Linux. Download the pre-compiled binary from the [release page](https://github.com/dwpeng/filterx/releases).


## Quick Start

it provides a simple expression language (like a subset of python programe language) to filter the csv file. There is a simple csv file `data.csv` as an example.

```csv
a,b,c
1,"a","apple"
2,"b","banana"
3,"b","dog"
4,"b","cat"
5,"e","elephant"
6,"f","fish"
7,"g","goat"
8,"h","horse"
9,"i","iguana"
10,"j","jaguar"
```

### Filter by column name and value

```bash
# -H means the file has a header
filterx data.csv -H "a == 1"
```

The output is

```csv
a,b,c
1,"a","apple"
```

### Filter by multiple conditions

```bash
filterx data.csv -H "a > 1 and b == 'b'"
```

The output is

```csv
a,b,c
2,"b","banana"
3,"b","dog"
4,"b","cat"
```

### Filter by a list of values

```bash
filterx data.csv -H "b in ('b', 'f')"
```

The output is

```csv
a,b,c
2,"b","banana"
3,"b","dog"
4,"b","cat"
6,"f","fish"
```

it also supports filter by a file which contains a list of values. For example, there is a file `filter.txt` which contains the following values.
```txt
b
f
```

```bash
# $1 means the first column in the file
filterx data.csv -H "b in 'filter.txt$1'"
```

### Column choosing

if one csv file has header, you can directly use the column name to filter the file. If the file does not have a header, you can use the column index to filter the file. For example, the following command filters the file by the first column.

```bash
# col(1) means the first column
filterx data.csv "col(1) > 1"
```

### Create a new column
```bash
filterx data.csv -H "alias('new') = 1;alias('fk') = a + 5"
```

The output is

```csv
a,b,c,new,fk
1,"a","apple",1,6
2,"b","banana",1,7
3,"b","dog",1,8
4,"b","cat",1,9
5,"e","elephant",1,10
6,"f","fish",1,11
7,"g","goat",1,12
8,"h","horse",1,13
9,"i","iguana",1,14
10,"j","jaguar",1,15
```

### Select columns to output

```bash
filterx data.csv -H "a > 1 and b == 'b';select(a)"
```

The output is

```csv
a
2
3
4
```

It also can used to control the order of columns.

```bash
filterx data.csv -H "a > 1 and b == 'b';select(c, a)"
```

The output is

```csv
c,a
"banana",2
"dog",3
"cat",4
```

### Drop columns

```bash
filterx data.csv -H "a > 1 and b == 'b';drop(a)"
```

The output is

```csv
b,c
"b","banana"
"b","dog"
"b","cat"
```


## Expression Language

The expression language is a subset of python language. It supports the following operators and functions.

### Operators

- `==`: equal
- `!=`: not equal
- `>`: greater than
- `>=`: greater than or equal
- `<`: less than
- `<=`: less than or equal
- `and`: logical and
- `or`: logical or
- `in`: in a list/file
- `not in`: not in a list/file

### Functions
- `col(n)`: get the value of the n-th column
- `alias(c)`: create column with name c
- `select(c, ...)`: select column with name c to output, support multiple columns
- `drop(c, ...)`: drop column with name c, support multiple columns


## Future

I am a graduate student studying bioinformatics and I need to filter large csv files regularly. I will add more features to this tool. If you have any suggestions, please let me know.


## License
MIT
