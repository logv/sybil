
PID=$$
TOTAL=0
NUM=10
ITERS=1000
for iter in `seq $ITERS`; do
  python scripts/fakedata/host_generator.py $NUM | ./bin/sybil ingest -table testingest >> ingest.${PID}.log 2>&1
  if [ $? -ne 0 ]; then
    echo "PROBLEM WITH INGESTION!!!!!", $PID
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
      echo "PROBLEM WITH DIGESTION DIGESTION DIGESTION!!!!!", $PID
      break
    fi
  fi

done
