TABLE=$1

if [ -z $1 ]; then

  echo "Usage: bash $0 [TABLE_NAME]"
  exit
fi

python scripts/fakedata/activity_generator.py 100000 | ./bin/sybil ingest -table $TABLE
python scripts/fakedata/activity_generator.py 1000000 | ./bin/sybil ingest -table ${TABLE}_extra
python scripts/fakedata/activity_join_generator.py | ./bin/sybil ingest -table ${TABLE}_info
