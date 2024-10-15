csv:
  cargo run --release -- c -H --oH test_data/test.csv -e "select(a);Alias(a) = 'sss'" out.csv
