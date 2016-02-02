# columns to add:
# time, session_id, int_id
# state, company, city, country
# name
# last name
# fullname
# int canary / str canary
import sys
import time
import random
import json

from faker import Faker
fake = Faker()



def rand_record():
    record = {
        "ints" : {},
        "strs" : {}
    }

    time_allowance = 60 * 60 * 24 * 7 * 4 # 1 month?
    record["ints"]["time"] = int(time.time()) + random.randint(-time_allowance, time_allowance)

    record["strs"]["name"] = fake.name()
    session_id = random.randint(0, 5000000)
    record["strs"]["session_id"] = str(session_id)
    record["strs"]["company"] = fake.company()
    record["strs"]["city"] = fake.city()
    record["strs"]["state"] = fake.state()
    record["strs"]["country"] = fake.country()
    canary = random.randint(0, 1000000)
    record["strs"]["str_canary"] = str(canary)

    record["ints"]["int_id"] = session_id
    record["ints"]["int_canary"] = canary
    record["ints"]["age"] =  abs(random.gauss(35, 15))
    record["ints"]["f1"] = random.randint(0, 50)
    record["ints"]["f2"] = random.randint(0, 500)
    record["ints"]["f3"] = random.gauss(1000000, 10000)


    return record

def generate_records(n):
    records = []
    for j in xrange(n):
        record = rand_record()
        records.append(record)
        print json.dumps(record)


    return records

NUM_RECORDS = 10000
if len(sys.argv) > 1:
    NUM_RECORDS = int(sys.argv[1])

generate_records(NUM_RECORDS)
