#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = "Wei Wang"
__email__ = "tskatom@vt.edu"


from operator import itemgetter


with open('topic_frequency.txt', "r") as topic_f:
    lines = topic_f.readlines()[1:]

    tp_fre = [(int(line.replace('"', "").replace("\n", "").split(" ")[0]) -1, float(line.replace('"', "").replace("\n", "").split(" ")[1])) for line in lines]
    tp_fre.sort(key=itemgetter(1), reverse=True)

    for t_q in tp_fre:
        print t_q

if __name__ == "__main__":
    pass
