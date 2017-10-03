import json
import random
import sys
import time

HOSTS = [ "www.facebook.com", "www.yahoo.com", "www.google.com", "www.reddit.com", "github.com" ]
STATII = [200, 403, 404, 500, 503]

NUM_RECORDS = 10000
if len(sys.argv) > 1:
    NUM_RECORDS = int(sys.argv[1])


IDX=0
def rand_record():
    record = { }

    global IDX

    rand_host = random.choice(HOSTS)
    record["status"] = str(random.choice(STATII))
    record["host"] = rand_host
    record["ping"] = abs(random.gauss(60, 20))
    record["weight"] = random.choice([1, 10, 100])
    time_allowance = 60 * 60 * 24 * 7 * 4 # 1 month?
    record["time"] = int(time.time()) + random.randint(-time_allowance, time_allowance)
    record["index_int"] = IDX
    record["index_str"] = str(IDX)
    record["groups"] = []

    if IDX % 2 == 0:
        record["groups"].append("mod2")

    if IDX % 3 == 0:
        record["groups"].append("mod3")

    if IDX % 5 == 0:
        record["groups"].append("mod5")

    if len(record["groups"]) == 0:
        record["groups"].append("none")


    IDX += 1
    return record

def generate_records(n):
    records = []
    for j in xrange(n):
        record = rand_record()
        records.append(record)
        print json.dumps(record)


    return records

generate_records(NUM_RECORDS)
