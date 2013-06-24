#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = "Wei Wang"
__email__ = "tskatom@vt.edu"


from etool import args, logs, queue
import re
import json
import codecs
from bs4 import BeautifulSoup as bs


__processor__ = 'news_filter'
log = logs.getLogger(__processor__)


url_pa = re.compile("http://(www\.){0,1}[^/]*/[a-z0-9/.\-]*(econ|finan|negocios|)[a-z0-9\.\-]*", flags=re.I)


def create_label_rule(r_file):
    rules = json.load(open(r_file, 'r'))
    r = "("
    for stock in rules:
        for e in rules[stock]:
            e.replace("\\.", "\\\\.")
            if e.find(" ") < 0:
                s_rule = " " + e + " " + "|"
            else:
                s_rule = e + "|"
            r += s_rule

    r = r[0:len(r) - 1] + ")"
    return r


def get_title(raw_html):
    try:
        soup = bs(raw_html)
        title = soup.title.text
        return title
    except:
        log.exception("Extract title error")
        return None


def create_gold_lable(r_file):
    rules = json.load(open(r_file, 'r'))
    gold_rule = {}
    for stock in rules:
        for e in rules[stock]:
            gold_rule[e.lower()] = stock

    return gold_rule


def filter_by_url(n_url, pattern):
    result = pattern.search(n_url)
    if result is not None:
        return True
    else:
        return False


def label_news(content, pattern, gold_rule):
    result = pattern.findall(content)
    result = [r.lower().strip() for r in result]
    if result is not None:
        result = {}.fromkeys(result).keys()

    labels = []
    for r in result:
        if r in gold_rule:
            labels.append(gold_rule.get(r))
    return labels, result


def process(news, u_pattern, c_pattern, gold_rule):
    n_url = news["url"]
    n_date = news['date']
    title = news.get('title', None)

    lan = news['BasisEnrichment']['language']
    #o_country = news.get('country', '--')
    if "embersGeoCode" in news:
        if "country" in news["embersGeoCode"]:
            o_country = news["embersGeoCode"]['country'].lower()
        else:
            o_country = "--"
    else:
        o_country = "--"

    content = " ".join([w['lemma'] for w in news['BasisEnrichment']['tokens']])

    url_fetch = filter_by_url(n_url, u_pattern)
    if url_fetch:
        labels, matched = label_news(content, c_pattern, gold_rule)
        if len(labels) > 0:
            if title is None:
                title = get_title(news['content'])
            f_news = {"url": n_url, "date": n_date, "title": title, "lan": lan, "o_country": o_country, "p_country": labels}
            return f_news

    return None


def main():
    ap = args.get_parser()
    ap.add_argument('--r_file', type=str, help="The rule file")
    ap.add_argument('--o', type=str, help="The output file")
    arg = ap.parse_args()

    assert arg.r_file, 'Need a rule file'
    assert arg.sub, 'Need a queue to subscribe'
    assert arg.o, 'Need a file to output'

    logs.init(arg)
    queue.init(arg)

    u_pattern = re.compile("http://(www\.){0,1}[^/]*/[a-z0-9/.\-]*(econ)[a-z0-9\.\-]*", flags=re.I)
    c_rule = create_label_rule(arg.r_file)
    g_rule = create_gold_lable(arg.r_file)
    c_pattern = re.compile(c_rule, flags=re.I)

    with queue.open(arg.sub, 'r') as q_r, codecs.open(arg.o, 'a') as f_a:
        for news in q_r:
            f_news = process(news, u_pattern, c_pattern, g_rule)
            if f_news is not None:
                f_a.write(json.dumps(f_news) + "\n")
                print f_news['date'], f_news['title'], "|", f_news['o_country'], "|", f_news["p_country"]


if __name__ == "__main__":
    main()
