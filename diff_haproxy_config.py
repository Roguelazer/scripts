#!/usr/bin/python

import argparse
import difflib
import re


_comment_re = re.compile(r'\s*#.*')
_blank_re = re.compile(r'^\s*$')
_start_block_re = re.compile(r'^(?P<type>\w+)( +(?P<name>[a-zA-Z0-9._-]+))?')


class Block(object):
    def __init__(self, ty, name):
        self.ty = ty
        self.name = name
        self.contents = set()

    @property
    def cmp_tuple(self):
        return (self.ty, self.name, frozenset(self.contents))

    def __eq__(self, other):
        return self.cmp_tuple == other.cmp_tuple

    def __ne__(self, other):
        return self.cmp_tuple != other.cmp_tuple

    def __repr__(self):
        return "Block<%s %s>" % (self.ty, self.name)

    @property
    def key(self):
        return (self.ty, self.name)

    def __hash__(self):
        return hash(self.key)


def parse_config(fd):
    blocks = {}
    current = None
    for line in fd:
        line = line.rstrip()
        line = _comment_re.sub('', line)
        if _blank_re.match(line):
            continue
        smd = _start_block_re.match(line)
        if smd:
            if current:
                blocks[current.key] = current
            current = Block(smd.group('type'), smd.group('name'))
        else:
            if not current:
                print line
            current.contents.add(line.lstrip())
    if current:
        blocks[current.key] = current
    return blocks


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('lhs', type=argparse.FileType('r'), help='Left-hand-side file')
    parser.add_argument('rhs', type=argparse.FileType('r'), help='Right-hand-side file')
    args = parser.parse_args()

    lhs = parse_config(args.lhs)
    rhs = parse_config(args.rhs)

    missing_in_lhs = set()
    missing_in_rhs = set()
    different = set()
    for key in (set(lhs.keys()) | set(rhs.keys())):
        if key in lhs and key in rhs:
            lblock = lhs[key]
            rblock = rhs[key]
            if lblock != rblock:
                different.add((lblock, rblock))
        elif key in lhs:
            missing_in_rhs.add(key)
        elif key in rhs:
            missing_in_lhs.add(key)
    if different:
        print "Differences:"
        for lblock, rblock in different:
            print
            fromfile = "%s %s" % (args.lhs.name, str(lblock.key))
            tofile = "%s %s" % (args.rhs.name, str(rblock.key))
            for line in difflib.unified_diff(list(sorted(lblock.contents)), list(sorted(rblock.contents)), fromfile=fromfile, tofile=tofile):
                print "\t%s" % line.rstrip()
    if missing_in_rhs:
        print "Removed in %s" % args.rhs.name
        print "\n".join("\t%s" % str(r) for r in missing_in_rhs)
    if missing_in_lhs:
        print "Added in %s" % args.rhs.name
        print "\n".join("\t%s" % str(l) for l in missing_in_lhs)
    if not (different or missing_in_lhs or missing_in_rhs):
        print "No diffs! Congrats!"


if __name__ == '__main__':
    main()
