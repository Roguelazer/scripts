#!/usr/bin/env python

# Compare two tables which are expected to be equal. Will only check columns defined on the left-hand table.
# Useful when using online-schema-change tools to add columns or whatnot.

import argparse
import os.path

import MySQLdb
import MySQLdb.cursors


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--id-column', default='id', help='Name of ID column (default %(default)s)')
    parser.add_argument('--limit', type=int, default=1000, help='Limit of number of differences to show')
    parser.add_argument('database')
    parser.add_argument('table1')
    parser.add_argument('table2')
    args = parser.parse_args()

    conn = MySQLdb.connect(
        read_default_file=os.path.expanduser('~/.my.cnf'), db=args.database,
        cursorclass=MySQLdb.cursors.DictCursor
    )

    c = conn.cursor()
    c.execute('SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=%s', (args.table1,))
    all_columns = [r['COLUMN_NAME'] for r in c]
    if args.id_column not in all_columns:
        parser.error('--id-column must be present on source table')

    query = '''SELECT lhs.`{id_column}` AS identifier
FROM `{table}` lhs
LEFT OUTER JOIN `{rhs_table}` rhs ON lhs.`{id_column}` = rhs.`{id_column}`
WHERE
{where_clause}
LIMIT {limit}
'''.format(
        table=args.table1,
        rhs_table=args.table2,
        id_column=args.id_column,
        where_clause=' OR '.join('lhs.`{col}` <> rhs.`{col}`'.format(col=col) for col in all_columns),
        limit=args.limit
    )

    c.execute(query)
    mismatched_ids = [r['identifier'] for r in c]
    print('Mismatched IDs: {0} (limit {1})'.format(mismatched_ids, args.limit))

    for row_id in mismatched_ids:
        c.execute('SELECT * FROM {table} WHERE {id_column}=%s'.format(
            table=args.table1, id_column=args.id_column), (row_id)
        )
        lhs = c.fetchone()
        c.execute('SELECT * FROM {table} WHERE {id_column}=%s'.format(
            table=args.table2, id_column=args.id_column), (row_id)
        )
        rhs = c.fetchone()
        print('ID: {0}'.format(row_id))
        print('  LHS: {0!r}'.format(lhs))
        print('  RHS: {0!r}'.format(rhs))
        conn.rollback()


main()
