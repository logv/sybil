# A program to ingest and digest rapidly, used for stressing deadlocks and lock files

PID=$$
TOTAL=0
NUM=10
if [ "$#" -eq 1 ]; then
  NUM=$1
fi
ITERS=1000
for iter in `seq $ITERS`; do
  python scripts/fakedata/host_generator.py $NUM | ./bin/sybil ingest -debug -table testingest >> ingest.${PID}.log 2>&1
  if [ $? -ne 0 ]; then
    echo "INGESTION FAILED!!!!!", $PID
    break
  fi
  TOTAL=$((( $TOTAL + $NUM )))
  echo "TOTAL IS" $TOTAL
  sleep 0.01
done
