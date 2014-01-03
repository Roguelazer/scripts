import sys
import random


def read_dict():
    d = []
    with open("/usr/share/dict/words", "r") as f:
        for line in f:
            d.append(line.strip())
    return d


def maybe_mutate(word):
    if random.random() > 0.5:
        return word.capitalize()
    else:
        return word


def make_password(words):
    d = read_dict()
    return [maybe_mutate(random.choice(d)) for _ in range(words)]


if __name__ == "__main__":
    print " ".join(make_password(int(sys.argv[1])))
