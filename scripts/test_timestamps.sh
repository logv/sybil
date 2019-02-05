rm tsdb -fr
export H=0.25
plait.py --num 100000 scripts/plait/timestamp_generator.yaml | sybil ingest -dir tsdb -table test_${H}
export H=0.5
plait.py --num 100000 scripts/plait/timestamp_generator.yaml | sybil ingest -dir tsdb -table test_${H}
export H=1
plait.py --num 100000 scripts/plait/timestamp_generator.yaml | sybil ingest -dir tsdb -table test_${H}
export H=3
plait.py --num 100000 scripts/plait/timestamp_generator.yaml | sybil ingest -dir tsdb -table test_${H}
export H=6
plait.py --num 100000 scripts/plait/timestamp_generator.yaml | sybil ingest -dir tsdb -table test_${H}
export H=12
plait.py --num 100000 scripts/plait/timestamp_generator.yaml | sybil ingest -dir tsdb -table test_${H}
export H=24
plait.py --num 100000 scripts/plait/timestamp_generator.yaml | sybil ingest -dir tsdb -table test_${H}
export H=48
plait.py --num 100000 scripts/plait/timestamp_generator.yaml | sybil ingest -dir tsdb -table test_${H}

find tsdb -name "int_time.db" | sort -g | xargs ls -lah
