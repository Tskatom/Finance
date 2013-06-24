#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = "Wei Wang"
__email__ = "tskatom@vt.edu"


from etool import logs, queue, args
import json
from datetime import datetime

__processor__ = 'listen_warning'
logs.getLogger(__processor__)
SENT_WARNINGS = []


def check_ifexist(warning):
    eventDate = warning["eventDate"]
    eventType = warning["eventType"]
    population = warning["population"]

    if [eventDate, eventType, population] in SENT_WARNINGS:
        return True
    else:
        SENT_WARNINGS.append([eventDate, eventType, population])
        return False


def main():
    ap = args.get_parser()
    ap.add_argument('--out', help="the output file of warnings")
    arg = ap.parse_args()

    assert arg.sub, 'Need a queue to subcribe!'
    assert arg.out, 'Need a file to store warnings!'

    logs.init(arg)
    queue.init(arg)
    out_file =  arg.out

    with queue.open(arg.sub, 'r') as q_r:
        for m in q_r:
            with open(out_file, "a") as out_w:
                if not check_ifexist(m):
                    out_w.write(json.dumps(m) + "\n")
                else:
                    print "Duplicated Warnings"


if __name__ == "__main__":
    main()
