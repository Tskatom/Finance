#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = "Wei Wang"
__email__ = "tskatom@vt.edu"


import numpy as np
import os
from etool import args
import codecs


def result_analysis(r_dir):
    os.chdir(r_dir)
    #load theta and phi file
    theta = np.load(r_dir + os.sep + 'theta.txt.npy')
    phi = np.load(r_dir + os.sep + 'phi.txt.npy')

    #get the top theta and phi
    top_theta = []
    top_phi = []

    #order the topic proportion in each document from high to low
    for m, m_theta in enumerate(theta):
        temp = [(k, v) for k, v in enumerate(m_theta)]
        temp.sort(key=lambda temp: temp[1], reverse=True)
        top_theta.append((m, temp))
    #order the topic distrition over word from high to low
    for t, t_phi in enumerate(phi):
        temp = [(k, v) for k, v in enumerate(t_phi)]
        temp.sort(key=lambda temp: temp[1], reverse=True)
        top_phi.append((t, temp))

    #save ordered theta
    with codecs.open(r_dir + os.sep + "order_theta.txt", "w") as t_w:
        for m_topic in top_theta:
            t_w.write(str(m_topic) + "\n")

    #save ordered phi
    with codecs.open(r_dir + os.sep + "order_phi.txt", "w") as p_w:
        for t_word in top_phi:
            p_w.write(str(t_word) + "\n")

    #add the topic proportion to documents
    with codecs.open(r_dir + os.sep + "ar_news.txt", "r") as n_r, codecs.open(r_dir + os.sep + "up_ar_news.txt", "w") as n_w:
        for m, line in enumerate(n_r.readlines()):
            news = eval(line)
            news["topics"] = {}
            for topic in top_theta[m][1][0:5]:
                t = topic[0]
                weight = round(topic[1], 2)
                if weight > .0:
                    news["topics"][t] = weight
            n_w.write(str(news) + "\n")

    print "theta shape ", theta.shape
    print "phi shape", phi.shape

    #output the top K word in each Topic
    id2word = {}
    with codecs.open("wordmap.txt", "r") as w_r:
        for line in w_r:
            line = line.replace("\r", "").replace("\n", "").strip().split("\t")
            try:
                id = int(line[0].strip())
                word = line[1].strip()
                id2word[id] = word
            except:
                print line
                id2word[id] = ""
                continue

    with codecs.open("top_phi.txt", "w") as top_phi_w:
        for t_word in top_phi:
            top_phi_w.write("Topic %d:\n" % t_word[0])
            for word in t_word[1][0:50]:
                w = id2word.get(word[0])
                top_phi_w.write("\t%s\t%.5f\n" % (w, word[1]))


def main():
    ap = args.get_parser()
    ap.add_argument('--r', help='result file directory')
    arg = ap.parse_args()

    result_analysis(arg.r)


if __name__ == "__main__":
    main()
