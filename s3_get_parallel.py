#!/usr/bin/env python

import argparse
import boto
import fnmatch
import functools
import multiprocessing
import os
import os.path
import re
import tempfile
import sys

S3_URI_REGEX = re.compile(r'^s3://(?P<bucket>[^/]+)/(?P<key>.+)')


def expand_globs(files_or_globs):
    conn = boto.connect_s3()
    buckets = {}
    files = set()
    for item in files_or_globs:
        parsed = S3_URI_REGEX.match(item)
        if not parsed:
            raise ValueError("Invalid URI %r" % item)
        bucket_name = parsed.group('bucket')
        raw_key = parsed.group('key')
        if bucket_name not in buckets:
            buckets[bucket_name] = conn.get_bucket(bucket_name)
        bucket = buckets[bucket_name]
        if raw_key.count('*') == 0:
            files.add((bucket_name, bucket.get_key(raw_key).key.name))
        else:
            prefix = raw_key[:raw_key.index('*')]
            for key in bucket.list(prefix=prefix):
                if fnmatch.fnmatch(key.name, raw_key):
                    files.add((bucket_name, key.name))
    return list(sorted(files))


def download_key(key, target_dir, allow_overwrite=False):
    bucket_name, key_name = key
    conn = boto.connect_s3()
    output_name = os.path.join(target_dir, os.path.basename(key_name))
    if os.path.exists(output_name) and not allow_overwrite:
        return output_name, False
    with tempfile.NamedTemporaryFile(delete=True) as fd:
        bucket = conn.get_bucket(bucket_name)
        bucket.get_key(key_name).get_contents_to_file(fd)
        try:
            os.link(fd.name, output_name)
        except OSError as e:
            if e.errno == 17:
                os.unlink(output_name)
                os.link(fd.name, output_name)
            else:
                raise
        os.chmod(output_name, 0644)
    return output_name, True


def main():
    assert 'AWS_ACCESS_KEY_ID' in os.environ, '$AWS_ACCESS_KEY_ID must be in the environment'
    assert 'AWS_SECRET_ACCESS_KEY' in os.environ,\
        '$AWS_SECRET_ACCESS_KEY must be in the environment'
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-a',
        '--allow-overwrite',
        default=False,
        action='store_true',
        help='If passed, will re-download files which already exist and overwrite'
    )
    parser.add_argument(
        '-p',
        '--max-parallelism',
        default=multiprocessing.cpu_count(),
        type=int,
        help='Max # of workers to run (default %(default)s)'
    )
    parser.add_argument(
        '-t',
        '--target-dir',
        default=os.getcwd(),
        help='Dir to write output to (default .)'
    )
    parser.add_argument('files_or_globs', nargs='+', help='Set of either s3 paths or globs')
    args = parser.parse_args()

    files = expand_globs(args.files_or_globs)

    if not files:
        print >>sys.stderr, "No S3 keys matched that glob. Womp."
        return 1

    pool = multiprocessing.Pool(min(len(files), args.max_parallelism))
    try:
        for downloaded_file, actually_downloaded in pool.imap(
                functools.partial(
                    download_key,
                    target_dir=args.target_dir,
                    allow_overwrite=args.allow_overwrite,
                ),
                files):
            print downloaded_file, actually_downloaded
    except KeyboardInterrupt:
        pool.terminate()
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
