#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = "Wei Wang"
__email__ = "tskatom@vt.edu"


import sys
import os


def get_k(f_dir, k=100):
    f = f_dir + os.sep + "word_list.txt"
    out_f = f_dir + os.sep + "out_word.txt"
    i = 0
    with open(f, "r") as r, open(out_f, "w") as w:
        for line in r.readlines():
            vs = line.replace("\n", "").split("\t")
            word = vs[0]
            count = int(vs[1]) / 10
            for j in xrange(count):
                w.write(word + " ")

            i += 1
            if i > k - 1:
                break


def main():
    k = int(sys.argv[1])
    f_dir = sys.argv[2]
    get_k(f_dir, k)

if __name__ == "__main__":
    main()
