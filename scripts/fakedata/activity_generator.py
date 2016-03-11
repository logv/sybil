import json
import random
import sys
import time

ACTIONS = [ "pageload", "click", "pageunload", "chat" ]
PAGES = [ "login", "home", "friends", "settings", "feed", "groups", "explore", "404" ]

USER_IDS = 5000
NUM_RECORDS = 1000000
if len(sys.argv) > 1:
    NUM_RECORDS = int(sys.argv[1])


# record should contain:
# user ID, session ID, page, action
def rand_record():
    record = { }

    record["action"] = str(random.choice(ACTIONS))
    record["page"] = str(random.choice(PAGES))
    record["userid"] = "person" + str(random.randint(1, USER_IDS))

    record["weight"] = random.choice([1, 10, 100])
    time_allowance = 60 * 60 * 24 * 7 * 4 # 1 month?
    record["time"] = int(time.time()) - random.randint(0, time_allowance)
    return record

def generate_records(n):
    records = []
    for j in xrange(n):
        record = rand_record()
        print json.dumps(record)


    return records

if __name__ == "__main__":
    generate_records(NUM_RECORDS)
