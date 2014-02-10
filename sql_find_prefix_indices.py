#!/usr/bin/env python

import argparse
import collections
import re
import sys

_COMMENT_RE = re.compile(r'--.*$', re.M)
_EMPTYLINE_RE = re.compile(r'^\s*$', re.M)
_CREATE_TABLE_RE = re.compile(r'CREATE\s+TABLE\s+([a-z0-9A-Z_-]+)\s*\((.*)\)')
_CREATE_INDEX_RE = re.compile(r'''
    CREATE\s+INDEX\s+
    ([a-z0-9A-Z_-]+)\s+
    ON\s+([a-z0-9A-Z_-]+)\s+
    USING\s+([a-z0-9A_Z_-]+)\s+
    (\((.*)\))?
''', re.X)


Index = collections.namedtuple('Index', ['index_name', 'table_name', 'index_type', 'columns'])


def lex_statement(s):
    s = _EMPTYLINE_RE.sub('', s)
    s = s.replace('\n', ' ')
    s = s.strip()
    return s


def find_left_prefices(indices):
    """Take a list of indices and return left prefices

    Returns a list of families of left prefices"""
    prefixes = collections.defaultdict(list)
    seen_index_tuples = collections.defaultdict(list)
    btree_indices = [i for i in indices if i.index_type == 'btree']
    btree_indices.sort(key=lambda i: (len(i.columns), i.columns))
    for index in btree_indices:
        for i in range(1, len(index.columns) + 1):
            left_prefix = tuple(index.columns[:i])
            if left_prefix in seen_index_tuples:
                prefixes[seen_index_tuples[left_prefix][0]].append(index)
        seen_index_tuples[index.columns].append(index)
    return prefixes


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('schema_file')
    args = parser.parse_args()

    statements = []
    buf = []
    with open(args.schema_file, 'r') as f:
        while True:
            res = f.read(8192)
            if not res:
                break
            if _COMMENT_RE.search(res):
                res = _COMMENT_RE.sub('', res)
            while ';' in res:
                buf.append(res[:res.index(';') + 1])
                statements.append(''.join(buf))
                buf = []
                res = res[res.index(';') + 1:]
        if buf:
            statements.append(''.join(buf))
    statements = map(lex_statement, statements)

    tables = {}
    indices = collections.defaultdict(list)
    for s in statements:
        cmd = _CREATE_TABLE_RE.search(s)
        imd = _CREATE_INDEX_RE.search(s)
        if cmd:
            table_name = cmd.group(1)
            tables[table_name] = cmd.group(2)
        elif imd:
            index_name = imd.group(1)
            table_name = imd.group(2)
            index_type = imd.group(3)
            maybe_columns = imd.group(5)
            if maybe_columns:
                maybe_columns = tuple(map(str.strip, maybe_columns.split(',')))
            else:
                maybe_columns = tuple()
            indices[table_name].append(Index(
                index_name,
                table_name,
                index_type,
                maybe_columns
            ))

    for table_name, table_indices in sorted(indices.items()):
        prefixes = find_left_prefices(table_indices)
        if prefixes:
            print 'Found duplicate indices for table %s' % table_name
            for prefix, suffixes in prefixes.items():
                print '\t%s (%s) is a left prefix of:' % (
                    prefix.index_name,
                    ','.join(prefix.columns),
                )
                for suffix in suffixes:
                    print '\t\t%s (%s)' % (
                        suffix.index_name,
                        ','.join(suffix.columns)
                    )


if __name__ == '__main__':
    sys.exit(main())
