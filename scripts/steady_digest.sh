# A program to ingest and digest rapidly, used for stressing deadlocks and lock files

PID=$$
TOTAL=0
NUM=10
ITERS=1000
for iter in `seq $ITERS`; do
  sleep 10
  echo "DIGESTING ITER $iter"
  ./bin/sybil digest -debug -table testingest >> digest.${PID}.log 2>&1
  echo "DONE"
  if [ $? -eq 0 ]; then
    continue      
  else
    echo "DIGESTION FAILED!!!!!", $PID
    break
  fi

done
