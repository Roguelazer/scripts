#!/usr/bin/env python

import argparse
import collections
import re
import sys

_COMMENT_RE = re.compile(r'--.*$', re.M)
_ATTRIBUTE_RE = re.compile(r'[a-zA-Z0-9_:+-]+')

Table = collections.namedtuple('Table', ['table_name', 'columns'])
Index = collections.namedtuple('Index', ['index_name', 'table_name', 'index_type', 'columns'])
Other = collections.namedtuple('OtherStatement', ['stuff'])


### Statements := <Statement> (";" <Statement>)*
### Statement := ("CREATE INDEX" <name> ON <name> USING <name> "("<C>)) |
###              ("CREATE TABLE <name> "("<C>")") |
### C := <S> ("," <S>)*
### S := (("PRIMARY"|"UNIQUE") "KEY" <K>) | ("CONSTRAINT" <M>) | (<name> <M>)
### K := <name> <kols>
### kols := "(" <name> ("," <name>)* ")"
### name := "`"? {token} "`"?
### M := ( "(" <M> ")" ) | {token}<M>


ColumnDefinition = collections.namedtuple('ColumnDefinition', ['column_name', 'definition'])
IndexDefinition = collections.namedtuple('IndexDefinition', ['idx_type', 'idx_name', 'columns'])


def _tokenize_statement(s):
    tokens = []
    this_token = []
    _save_split_chars = ('`', ',', ';', '(', ')')
    _quotechars = ('"', "'")
    s = _COMMENT_RE.sub('', s)
    inchar = ''
    for char in s:
        if inchar:
            if char == inchar:
                if this_token:
                    tokens.append(''.join(this_token))
                this_token = []
                inchar = ''
            else:
                this_token.append(char)
        else:
            if char in _quotechars:
                inchar = char
            elif char in _save_split_chars:
                if this_token:
                    tokens.append(''.join(this_token))
                this_token = []
                tokens.append(char)
            elif char.isspace():
                if this_token:
                    tokens.append(''.join(this_token))
                this_token = []
            else:
                this_token.append(char)
    return tokens


def _take_until(s, needle):
    idx = s.index(needle)
    return s[:idx], s[idx:]


def _consume(s, *needles):
    for needle in needles:
        assert s[0] == needle
        s = s[1:]
    return s


def parse_name(s):
    if s[0] == '`':
        s = _consume(s, '`')
        nom = s[0]
        s = _consume(s[1:], '`')
    else:
        nom = s[0]
        s = s[1:]
    return nom, s


def parse_kols(s):
    kols = []
    s = _consume(s, '(')
    while s[0] != ')':
        kol_name, s = parse_name(s)
        kols.append(kol_name)
        if s[0] == ',':
            s = _consume(s, ',')
        else:
            break
    s = _consume(s, ')')
    return tuple(kols), s


def parse_k(s):
    index_type, rest = _take_until(s, 'KEY')
    index_type = index_type[0] if index_type else ''
    rest = _consume(rest, 'KEY')
    if index_type != 'PRIMARY':
        index_name, rest = parse_name(rest)
    else:
        index_name = '(primary)'
    columns, rest = parse_kols(rest)
    return IndexDefinition(
        index_type,
        index_name,
        columns
    ), rest


def parse_matched_parentheses(s, extra_exit=tuple()):
    p = []
    while True:
        if not s:
            break
        elif s[0] == ')':
            break
        elif s[0] in extra_exit:
            break
        else:
            p.append(s[0])
            if s[0] == '(':
                p_1, s = parse_matched_parentheses(s[1:])
                p.extend(p_1)
                s = _consume(s, ')')
                p.append(')')
            else:
                s = s[1:]
    return p, s


def parse_s(s, expect_end=None):
    if 'KEY' in s[0:3]:
        return parse_k(s)
    elif s[0] == 'CONSTRAINT':
        p, s = parse_matched_parentheses(s)
        return Other(p), s
    else:
        column_name, s = parse_name(s)
        definition, s = parse_matched_parentheses(s, [';', ','])
        return ColumnDefinition(column_name, definition), s


def parse_c(s, e=None):
    statements = []
    while s:
        parsed, s = parse_s(s, e)
        statements.append(parsed)
        if not s or s[0] != ',':
            break
        s = _consume(s, ',')
    return statements, s


def parse_statement(s):
    rv = []
    if s[:2] == ['CREATE', 'TABLE']:
        s = s[2:]
        table_name, s = parse_name(s)
        s = _consume(s, '(')
        body, s = parse_c(s, ')')
        s = _consume(s, ')')
        table_specs, s = _take_until(s, ';')
        columns = []
        for definition in body:
            if isinstance(definition, ColumnDefinition):
                columns.append(definition)
            elif isinstance(definition, IndexDefinition):
                rv.append(Index(
                    definition.idx_name,
                    table_name,
                    'btree',
                    tuple(definition.columns)
                ))
        rv.append(Table(table_name, columns))
        return rv, s
    elif s[:2] == ['CREATE', 'INDEX']:
        index_name, s = parse_name(s[2:])
        s = _consume(s, 'ON')
        table_name, s = parse_name(s)
        s = _consume(s, 'USING')
        index_type = s[0]
        s = _consume(s[1:], '(')
        columns, s = parse_c(s)
        s = _consume(s, ')')
        if s[0] in ('WHERE', 'WITH'):
            p, s = parse_matched_parentheses(s, [';'])
        idx = Index(
            index_name,
            table_name,
            index_type,
            tuple([c.column_name for c in columns])
        )
        return [idx], s
    else:
        parsed, rest = _take_until(s, ';')
        return [Other(parsed)], rest


def parse_statements(s):
    statements = []
    while s:
        parsed, s = parse_statement(s)
        statements.extend(parsed)
        if not s or s[0] != ';':
            break
        s = _consume(s, ';')
    return statements, s


def parse_top(s):
    tokens = _tokenize_statement(s)
    statements, rest = parse_statements(tokens)
    assert not rest
    return statements


def find_left_prefices(indices):
    '''Take a list of indices and return left prefices

    Returns a list of families of left prefices'''
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

    data = []
    with open(args.schema_file, 'r') as f:
        data = f.read()

    tables = {}
    indices = collections.defaultdict(list)
    statements = parse_top(data)
    for statement in statements:
        if isinstance(statement, Table):
            tables[statement.table_name] = statement
        elif isinstance(statement, Index):
            indices[statement.table_name].append(statement)

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
