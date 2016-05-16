# A program to ingest and digest rapidly, used for stressing deadlocks and lock files

PID=$$
TOTAL=0
NUM=10
ITERS=1000
for iter in `seq $ITERS`; do
  python scripts/fakedata/host_generator.py $NUM | ./bin/sybil ingest -table testingest >> ingest.${PID}.log 2>&1
  if [ $? -ne 0 ]; then
    echo "INGESTION FAILED!!!!!", $PID
    break
  fi
  TOTAL=$((( $TOTAL + $NUM )))
  echo "TOTAL IS" $TOTAL
  sleep 0.01
  if (( RANDOM % 10 == 0 )); then
    ./bin/sybil digest -table testingest >> digest.${PID}.log 2>&1
    if [ $? -eq 0 ]; then
      continue      
    else
      echo "DIGESTION FAILED!!!!!", $PID
      break
    fi
  fi

done
