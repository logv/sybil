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

  flaghash=$(echo ${flags} | md5sum |awk '{print $1}')
  cat f1.gob | go run scripts/flaggob2json.go > "workdir/query/corpus/flags-${flaghash}.json"

  cat f1.gob | sybil query -decode-flags -encode-results > results/r1.gob
  sybil query -dir db-2 -table a -encode-flags ${flags}> f2.gob
  cat f2.gob | sybil query -decode-flags -encode-results > results/r2.gob
  cat f1.gob | sybil aggregate results
}

testagg -info
testagg -tables
testagg -samples
testagg -int ping -group host -op hist
testagg -int ping -group host -op distinct
testagg -int ping -group host -export
testagg -int ping -group host -read-log
testagg -int ping -group host -recycle-mem=false
testagg -int ping -distinct host
testagg -int time -group host -op hist
testagg -int time -group host -op hist -loghist
testagg -group status,host -int ping -json -op hist -time -int-filter "time:lt:`date --date=\"-1 week\" +%s`"
testagg -str host -group host -loghist
testagg -int ping,status_code -str host -group host
testagg -int ping -str status -group status -sort ping
set +e
testagg -int ping -group host -sort status
set -e

# cleanup
rm -rf db-{1,2} f{1,2}.gob results
