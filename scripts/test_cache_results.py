# this file is for generating cached query timing results 
from __future__ import print_function

import subprocess
import plaitpy
import shlex
import json
import os
import shutil
import time
import tabulate


TEMPLATE="./scripts/plait/host_generator.yaml"
N=5000000
TEST_TABLE="cache_results_"

DIRS = [
    [(100 * 1000), "100k"],
    [(200 * 1000), "200k"],
    [(500 * 1000), "500k"],
    [(1000 * 1000), "1mm"],
    [(2000 * 1000), "2mm"],
    [(5000 * 1000), "5mm"],
    [(10 * 1000 * 1000), "10mm"],
    [(20 * 1000 * 1000), "20mm"],
]


QUERIES=[
    ("", "COUNT(*)"),
    ("-int ping", "AVG(ping)"),
    ("-int ping -op hist", "HIST(ping)"),
    ("-int ping -time-col time -time -time-bucket 21600", "AVG(ping) GROUP BY BUCKET(time, 21600) "),
    ("-group host", "GROUP BY host"),
    ("-distinct host", "COUNT DISTINCT(host)"),
    ("-distinct host -time-col time -time -time-bucket 21600", "COUNT DISTINCT(host) GROUP BY BUCKET(time, 21600)"),
    ("-group host -int ping", "AVG(ping) GROUP BY host"),
    ("-group host -int ping -limit 10", "AVG(ping) GROUP BY host LIMIT 10"),
    ("-group host -int ping -op hist", "HIST(ping) GROUP BY host"),
    ("-group host -str-filter host:re:facebook|google -int ping", "AVG ping GROUP BY host WHERE host ~= facebook|google"),
    ("-group host,status", "GROUP BY host,status"),
    ("-group host,status -int ping", "AVG ping GROUP BY host, status"),
    ("-group host -int ping -time-col time -time -time-bucket 21600 -limit 10", "AVG ping GROUP BY host, BUCKET(time, 21600) LIMIT 10"),
    ("-group host -int ping -time-col time -time -time-bucket 21600 -limit 100", "AVG ping GROUP BY host, BUCKET(time, 21600) LIMIT 100"),
]

DB_DIR = "testdb/%s" % (TEST_TABLE)

def gen_and_ingest_data(test_table, n):
    incr = int(1e5)
    t = plaitpy.Template(TEMPLATE)
    for i in xrange(0, n, incr):
        records = t.gen_records(incr)
        ingest_data(test_table, records, i)
    

def gen_data(n):
    print("GENERATING", n, "RECORDS")
    t = plaitpy.Template(TEMPLATE)
    records = t.gen_records(n)
    return records


def clean_dir(dir):
    print("REMOVING DB FOLDER", dir)
    try:
        shutil.rmtree(dir)
    except:
        pass

def clean_cache():
    ot = subprocess.check_output(shlex.split("find ./testdb -name cache"))
    for line in ot.split("\n"):
        if line:
            shutil.rmtree(line)

def ingest_data(test_table, records, depth=0):
    if depth:
        print(depth, "INGESTING RECORDS INTO", test_table)
    else:
        print("INGESTING RECORDS INTO", test_table)

    p = subprocess.Popen(["sybil", "ingest", "-table", test_table, "-dir", "testdb"], 
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, close_fds=True)
    output_str = "\n".join([str(r).replace("'", '"') for r in records])
    stdout, stderr = p.communicate(output_str)

    subprocess.check_output(shlex.split("sybil digest -table %s -dir testdb" % test_table))

def evict_page_cache():
    subprocess.check_output(["vmtouch", "-e", "testdb"])

def run_queries():

    def ms(t):
        return round(t * 1000, 2)

    series = {}
    tables = []
    for i,q in enumerate(QUERIES):
        if type(q) == tuple:
            q, d = q
        else:
            d = q
        s1 = []
        s2 = []
        s3 = []
        max_y = 0

        table = { "query" : q, "display" : d}

        tables.append(table)
        for count, suffix in DIRS:
            print("TESTING", suffix)
            test_table = "cache_results_%s" % suffix
            print("  QUERY '%s'" % q)

            evict_page_cache()
            args = ("sybil query -dir testdb -table %s" % test_table).split()
            args.extend(shlex.split(q))
            c_args = args + ["--cache-queries"]
            start = time.time()
            output = subprocess.check_output(c_args)
            end = time.time()

            print("  INITIAL CACHING QUERY TOOK", ms(end - start))
            s2.append([count, (end-start)])

            evict_page_cache()
            start = time.time()
            output = subprocess.check_output(args)
            end = time.time()

            print("  NON CACHED TOOK", ms(end - start))
            s1.append((count, end-start))

            evict_page_cache()
            start = time.time()
            output = subprocess.check_output(c_args)
            end = time.time()
            print("  SUBSEQUENT CACHED QUERY TOOK", ms(end - start))
            s3.append((count, end-start))

            max_y = max(s3[-1][1], s2[-1][1], s1[-1][1], max_y)

            table[suffix] = (s3[-1][1], s1[-1][1], s2[-1][1])

        series["%s_%s" % (q, "nocache")] = s1
        series["%s_%s" % (q, "initial")] = s2
        series["%s_%s" % (q, "subsequent")] = s3



        import matplotlib.pyplot as plt

        x1 = [s[0] for s in s1]
        y1 = [s[1] for s in s1]
        x2 = [s[0] for s in s2]
        y2 = [s[1] for s in s2]
        x3 = [s[0] for s in s3]
        y3 = [s[1] for s in s3]

        plt.close()

        fig = plt.figure()
        ax = fig.add_subplot(1,1,1)
        if max_y < 1:
            max_y = 1
        elif max_y < 5:
            max_y = 5
        elif max_y <= 10:
            max_y = 10
        elif max_y <= 20:
            max_y = 20
        elif max_y <= 30:
            max_y = 30
        else:
            max_y = max_y
        ax.set_ylim(top=max_y)
        ax.semilogx(x1, y1, x2, y2, x3, y3, marker='x')
        ax.set_title("#%i: %s" % (i, d))
        ax.legend(["uncached query", "query + save to cache", "cached query"])
#        ax.yaxis.set_major_formatter(FormatStrFormatter('%01.2f'))

        plt.savefig("figs/%02i_%s" % (i, q.replace(" ", "_")))

    return series, tables

def save_tables(tables):
    try:
        os.makedirs("figs/tables/")
    except:
        pass

    for count, suffix in DIRS:
        table = []
        for i, q in enumerate(tables):
            row = [i, q["display"]]
            row.extend(q[suffix])
            table.append(row)

        r = tabulate.tabulate(table, ["ID", "Query", "Cached Query", "Fresh Query", "Query + Save"], tablefmt="latex")

        with open("figs/tables/%s.tex" % suffix, "w") as f:
            f.write(r)

def gen_all_data():
    for count,suffix in DIRS:
        test_table = "cache_results_%s" % suffix
        gen_and_ingest_data(test_table, count)


RESET = os.environ.get("RESET", False)
GEN = os.environ.get("GEN", False)
def main():
    if RESET:
        clean_dir("testdb")
    else:
        clean_cache()

    if RESET or GEN:
        gen_all_data()

    series, tables = run_queries()
    save_tables(tables)

if __name__ == "__main__":
    main()
