#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = "Wei Wang"
__email__ = "tskatom@vt.edu"


import codecs
from etool import args


def compute_topic_daily(data_file, topic_num):
    daily_topics = {}
    with codecs.open(data_file, "r") as r:
        for line in r:
            doc = eval(line)
            #get post Date
            post_date = doc["postTime"][0:10]
            daily_topics.setdefault(post_date, {})
            #get topics in this document
            doc_topic = doc["topics"]
            #add the topic weight to daily topic
            for ele in doc_topic:
                daily_topics[post_date].setdefault(ele, 0.0)
                daily_topics[post_date][ele] += doc_topic[ele]

    #write the topic trend to csv file
    sorted_dates = sorted(daily_topics)
    with codecs.open("topic_trend.txt", "w") as w:
        title_str = "date|"
        for k in range(topic_num):
            title_str += str(k) + "|"
        title_str = title_str[0:len(title_str) - 1] + "\n"
        w.write(title_str)
        for post_date in sorted_dates:
            topic_distribution = daily_topics[post_date]
            data_str = post_date + "|"
            summary = sum(topic_distribution.values())
            for k in range(topic_num):
                weight = float(topic_distribution.get(k, .0)) / summary
                data_str += "%0.4f|" % weight
            data_str = data_str[0:len(data_str) - 1] + "\n"
            w.write(data_str)


def main():
    ap = args.get_parser()
    ap.add_argument("--da", help="the updated news file for analyze")
    ap.add_argument("--k", type=int, help="the number of topic")
    arg = ap.parse_args()

    assert arg.da, "Please input a news file"
    assert arg.k, "Please input the number of topics"

    compute_topic_daily(arg.da, arg.k)

if __name__ == "__main__":
    main()
