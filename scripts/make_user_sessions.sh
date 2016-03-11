TABLE=$1

if [ -z $1 ]; then

  echo "Usage: bash $0 [TABLE_NAME]"
  exit
fi

python scripts/fakedata/activity_generator.py | ./bin/sybil ingest -table $TABLE
python scripts/fakedata/activity_join_generator.py | ./bin/sybil ingest -table ${TABLE}_info
