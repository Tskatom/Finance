#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = "Wei Wang"
__email__ = "tskatom@vt.edu"


from etool import logs, queue, args
import json
from datetime import datetime

__processor__ = 'listen_warning'
logs.getLogger(__processor__)


def main():
    ap = args.get_parser()
    ap.add_argument('--out', help="the output file of warnings")
    arg = ap.parse_args()

    assert arg.sub, 'Need a queue to subcribe!'
    assert arg.out, 'Need a file to store warnings!'

    logs.init(arg)
    queue.init(arg)
    out_file = "%sdelatcurrency-warnings-%s" % (arg.out, datetime.strftime(datetime.now(), "%Y-%m-%d-%H-%M-%S"))

    with queue.open(arg.sub, 'r') as q_r:
        for m in q_r:
            with open(out_file, "a") as out_w:
                print m
                out_w.write(json.dumps(m) + "\n")


if __name__ == "__main__":
    main()
