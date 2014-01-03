from __future__ import print_function

import argparse
import collections
import json
import mmap
import time
import struct
import random
import sys
import os

import psycopg2

PER_PROCESS_SHM_SIZE = 102400

QUERIES = (
    "SELECT id, name FROM user_tags WHERE id=%s",
    "SELECT usersid, email FROM users WHERE usersid=%s",
    #"SELECT usersid, notes FROM users WHERE email LIKE '%%%s%%'",
)


def gen_query(some_integer):
    return QUERIES[some_integer % len(QUERIES)], (some_integer,)


def runner(child_number, shm, args):
    random.seed()
    my_random_sleep = random.random() * args.sleep_time / 1000.0

    if random.random() < args.idle_fraction:
        idle = True
    else:
        idle = False

    # initialize the shm segment
    shm.seek(child_number * PER_PROCESS_SHM_SIZE)
    shm.write(struct.pack('@i', 0))

    # connect and run test
    start = time.time()
    results = collections.Counter()
    conn = None
    times = []
    target_time = time.time() + (args.run_time - 2)
    try:
        conn = psycopg2.connect("host=/var/run/postgresql dbname=%s" % args.dbname)
        if idle:
            time.sleep(target_time - time.time())
        else:
            i = 0
            while time.time() < target_time:
                i += 1
                results['attempted'] += 1
                cur = conn.cursor()
                row_id = (child_number * 2000) + i
                start_inner = time.time()
                cur.execute(*gen_query(row_id))
                for row in cur:
                    results['good'] += 1
                times.append(time.time() - start_inner)
                time.sleep(my_random_sleep)
                if i % 4 == 0:
                    conn.rollback()
        conn.close()
    except Exception:
        results['exception'] = True
        raise
    end = time.time()
    results = dict(results)
    if idle:
        results['idle_threads'] = 1
    else:
        results['avg_query_ms'] = 1000 * sum(times) / float(len(times))
    results['run_time'] = end - start

    # write out to the shm segment and go away
    encres = json.dumps(results).encode('utf-8')
    assert len(encres) < PER_PROCESS_SHM_SIZE
    shm.seek(child_number * PER_PROCESS_SHM_SIZE)
    shm.write(struct.pack('@i', len(encres)))
    shm.write(encres)
    os._exit(0)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-d',
        '--dbname',
        required=True,
        help='Name of the postgres database to connect to'
    )
    parser.add_argument(
        '-c',
        '--children',
        type=int,
        default=16,
        help='Number of children to spawn (default %(default)s)'
    )
    parser.add_argument(
        '-r',
        '--run-time',
        type=int,
        default=30,
        help='Max time to ru (default %(default)s)'
    )
    parser.add_argument(
        '-i',
        '--idle-fraction',
        type=float,
        default=0.1,
        help='Fraction of threads to leave idle. Should be in [0.0, 1.0) (default %(default)s)'
    )
    parser.add_argument(
        '-s',
        '--sleep-time',
        type=float,
        default=20,
        help='Number of ms for children to sleep between queries (default %(default)s)'
    )
    args = parser.parse_args()
    shm = mmap.mmap(-1, (args.children + 1) * PER_PROCESS_SHM_SIZE)

    children = {}
    live_children = set()

    # spawn our children
    for child_number in range(1, args.children + 1):
        pid = os.fork()
        if pid == 0:
            runner(child_number, shm, args)
            os._exit(0)
        else:
            children[pid] = child_number
            live_children.add(pid)

    start_runs = time.time()
    # let the tests run
    d = {'children': 0}
    while time.time() - start_runs < args.run_time:
        if not live_children:
            break
        pid, _ = os.waitpid(-1, os.WNOHANG)
        if pid == 0:
            time.sleep(0.1)
            continue
        else:
            live_children.remove(pid)
            d['children'] += 1
            child_number = children[pid]
            # read back the output
            shm.seek(child_number * PER_PROCESS_SHM_SIZE)
            bytes_output = struct.unpack('@i', shm.read(4))[0]
            output = json.loads(shm.read(bytes_output))
            for key, value in output.iteritems():
                if isinstance(value, int):
                    d.setdefault(key, 0)
                    d[key] += value
                elif isinstance(value, bool):
                    d.setdefault(key, 0)
                    d[key] += (1 if value else 0)
                else:
                    d.setdefault(key, 0)
                    d[key] += (value / args.children)
    else:
        print("timeout")
        for pid in live_children:
            try:
                os.kill(pid, 15)
                time.sleep(0.1)
                gotpid, _ = os.waitpid(pid, os.WNOHANG)
                if gotpid == 0:
                    os.kill(pid, 9)
                    os.waitpid(pid)
            except Exception as e:
                print(e)
                continue

    print(d)


if __name__ == '__main__':
    sys.exit(main())
