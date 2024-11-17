csv:
  cargo run --release -- c -H --oH test_data/test.csv -e "select(a);Alias(a) = 'sss'" out.csv

bin217:
  cargo zigbuild --target x86_64-unknown-linux-gnu.2.17 --release

publish:
    cargo publish -p filterx_core --registry crates-io
    cargo publish -p filterx_info --registry crates-io
    cargo publish -p filterx_source --registry crates-io
    cargo publish -p filterx_engine --registry crates-io
    cargo publish -p filterx --registry crates-io
