import json
import random
import sys
import time

BROWSERS = [ "firefox", "chrome" ,"ie", "safari" ]
MOBILE = [ True, False ]

from faker import Faker
fake = Faker()

import activity_generator
USER_IDS = activity_generator.USER_IDS
if len(sys.argv) > 1:
    USER_IDS = int(sys.argv[1])

def rand_record(n):
    record = { }

    # STRINGS
    record["name"] = fake.name()

    record["userid"] = "person" + str(n)
    record["company"] = fake.company()
    record["browser"] = random.choice(BROWSERS)
    record["city"] = fake.city()
    record["state"] = fake.state()
    record["country"] = fake.country()

    return record

def generate_records():
    records = []
    for j in xrange(1,USER_IDS+1):
        record = rand_record(j)
        print json.dumps(record)


    return records


if __name__ == "__main__":
    generate_records()
