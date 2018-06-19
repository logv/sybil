#!/bin/bash
set -euo pipefail
go get golang.org/x/tools/cmd/benchcmp
make benchmarks
mv bench.txt bench-new.txt
git checkout master
make benchmarks
mv bench.txt bench-old.txt
benchcmp bench-old.txt bench-new.txt |tee bench-cmp.txt
go get github.com/ajstarks/svgo/benchviz
cat bench-cmp.txt | benchviz > bench-cmp.svg
cp bench* /tmp/test-results
