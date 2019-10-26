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
    record = { }

    # STRINGS
    record["name"] = fake.name()

    session_id = random.randint(0, 5000000)
    record["session_id"] = str(session_id)
    record["company"] = fake.company()
    record["city"] = fake.city()
    record["state"] = fake.state()
    record["country"] = fake.country()

    canary = random.randint(0, 1000000)
    record["str_canary"] = str(canary)
    record["str_canary2"] = str(canary)
    record["str_canary3"] = str(canary)

    # INTS
    time_allowance = 60 * 60 * 24 * 7 * 4 # 1 month?
    record["time"] = int(time.time()) + random.randint(-time_allowance, time_allowance)
    record["time2"] = record["time"]
    record["time3"] = record["time"]

    record["int_id"] = session_id
    record["int_canary"] = canary
    record["int_canary_2"] = canary
    record["int_canary_3"] = canary

    record["age"] =  abs(random.gauss(35, 15))
    record["f1"] = random.randint(0, 50)
    record["f2"] = random.randint(0, 500)
    record["f3"] = random.gauss(1000000, 10000)


    return record

def generate_records(n):
    records = []
    for j in range(n):
        record = rand_record()
        records.append(record)
        print(json.dumps(record))


    return records

NUM_RECORDS = 10000
if len(sys.argv) > 1:
    NUM_RECORDS = int(sys.argv[1])

generate_records(NUM_RECORDS)
