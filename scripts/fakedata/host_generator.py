import json
import random
import sys
import time

HOSTS = [ "www.facebook.com", "www.yahoo.com", "www.google.com", "www.reddit.com", "github.com" ]
STATII = [200, 403, 404, 500, 503]

NUM_RECORDS = 10000
if len(sys.argv) > 1:
    NUM_RECORDS = int(sys.argv[1])


def rand_record():
    record = {
        "ints" : {},
        "strs" : {}
    }

    record["strs"]["status"] = str(random.choice(STATII))
    record["strs"]["host"] = random.choice(HOSTS)
    record["ints"]["ping"] = abs(random.gauss(60, 20))
    record["ints"]["time"] = int(time.time())

    return record

def generate_records(n):
    records = []
    for j in xrange(n):
        record = rand_record()
        records.append(record)
        print json.dumps(record)


    return records

generate_records(NUM_RECORDS)
