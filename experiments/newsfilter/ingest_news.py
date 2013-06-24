#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = "Wei Wang"
__email__ = "tskatom@vt.edu"


from etool import args, logs, queue
import json


__processor__ = 'ingest_news'
log = logs.getLogger(__processor__)


def main():
    ap = args.get_parser()
    ap.add_argument('--f', type=str, help='the newes file')

    arg = ap.parse_args()

    assert arg.f, 'Need a file to ingest'
    assert arg.pub, 'Need a queue to publish'

    logs.init(arg)
    queue.init(arg)

    with queue.open(arg.pub, 'w') as q_w, open(arg.f, 'r') as f_r:
        for line in f_r:
            news = json.loads(line)
            q_w.write(news)


if __name__ == "__main__":
    main()
