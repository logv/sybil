#!/bin/bash
set -euo pipefail

rm -f "$(which sybil)"
go install -v ./...

python scripts/fakedata/host_generator.py | sybil ingest -dir db-1 -table a
python scripts/fakedata/host_generator.py | sybil ingest -dir db-2 -table a
sybil digest -dir db-1 -table a
sybil digest -dir db-2 -table a

function testagg() {
  local flags=${*}
  test -d results || mkdir results
  sybil query -dir db-1 -table a -encode-flags ${flags}> f1.gob
  cat f1.gob | sybil query -decode-flags -encode-results > results/r1.gob

  sybil query -dir db-2 -table a -encode-flags ${flags}> f2.gob
  cat f2.gob | sybil query -decode-flags -encode-results > results/r2.gob
  cat f1.gob | sybil aggregate results
}

testagg -info | grep avg
testagg -int ping -group host -op avg
testagg -int ping -group host -op hist

# cleanup
rm -rf db-{1,2} f{1,2}.gob results
