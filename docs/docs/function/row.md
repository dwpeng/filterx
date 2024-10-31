
```csv title="test.csv
name,age
Alice,20
Bob,30
Charlie,40
```

## head

load/fetch the first `n` rows

```shell
filterx csv test.csv -H --oH -e 'head(2)'

# output
# name,age
# Alice,20
# Bob,30
```

## limit

outputs `n` rows starting from loaded row `m`

```shell
filterx csv test.csv -H --oH -e 'age > 10;limit(2)'

# output
# name,age
# Alice,20
# Bob,30
```

## tail

load/fetch the last `n` rows

```shell
filterx csv test.csv -H --oH -e 'tail(2)'
# output
# name,age
# Bob,30
# Charlie,40
```
