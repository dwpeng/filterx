csv:
  cargo run --release -- c -H --oH test_data/test.csv -e "select(a, b);Alias(c) = b + 'sss'"
