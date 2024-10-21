csv:
  cargo run --release -- c -H --oH test_data/test.csv -e "select(a);Alias(a) = 'sss'" out.csv

bin217:
  cargo zigbuild --target x86_64-unknown-linux-gnu.2.17 --release
