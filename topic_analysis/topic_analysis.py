#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Apply LDA to analyze the topics distribution of news headline,
Then applying Linear Regression to constrcut the relation between
Topic distribution of specified day windows to the fluctuation of stock market

"""

__author__ = "Wei Wang"
__email__ = "tskatom@vt.edu"

import numpy as np
import codecs
from etool import args
import os
import random


def sample_multinormial(p):
    "sample from a multinormial"
    r = round(random.random(), 4)
    for i, q in enumerate(p):
        r -= q
        if r < 0.0:
            return i
    return len(p) - 1


class Lda():
    def __init__(self, k=100, alpha=0.1, beta=0.1, iternum=500):
        self.alpha = alpha
        self.beta = beta
        self.K = k
        self.iternum = iternum
        self.word2id = {}
        self.id2word = {}

    def _initiate(self, doc_matrix):
        pass

    def doc_process(self, doc_file):
        with codecs.open(doc_file, encoding='ascii', mode='r') as d_r:

            self.docs = [eval(line)['title'].split(" ") for line in d_r]

        self.doc_matrix = np.array([np.zeros(len(x), dtype=float) for x in self.docs])

        "initiate the wordmap: constructure the mapping between id and word"
        for i, doc in enumerate(self.docs):
            for j, w in enumerate(doc):
                if w not in self.word2id:
                    id = len(self.word2id)
                    self.word2id[w] = id
                    self.id2word[id] = w
                    self.doc_matrix[i][j] = id
                else:
                    id = self.word2id.get(w)
                    self.doc_matrix[i][j] = id

        "initiate the parameters for topic model"
        #the size of corpus
        self.M = len(self.docs)
        #the size of vocabulary
        self.V = len(self.word2id)
        #the topic assignment for each word in document
        self.z = np.array([np.zeros(len(x), dtype=float) for x in self.doc_matrix])
        #the number of topics being assignment to  word w
        self.nw = np.zeros([self.V, self.K], dtype=float)
        #the number of topics besing assigment to document d
        self.nd = np.zeros([self.M, self.K], dtype=float)
        #the time of the topic being assigned
        self.sumt = np.zeros(self.K, dtype=float)
        #the total number of word
        self.sumd = np.zeros(self.M, dtype=float)
        #topic distribution over words
        self.phi = np.zeros([self.K, self.V], dtype=float)
        #topic proportation in document
        self.theta = np.zeros([self.M, self.K], dtype=float)

    def out_put_wordmap(self, out_d):
        with codecs.open(out_d + os.sep + "wordmap.txt", encoding='ascii', mode='w') as c_w:
            for i in xrange(self.V):
                c_w.write("%d \t %s\n" % (i, self.id2word.get(i)))

    def gib_sample(self, m, n):
        #get the old topic
        o_topic = self.z[m][n]
        w = self.doc_matrix[m][n]

        self.nw[w][o_topic] -= 1
        self.nd[m][o_topic] -= 1
        self.sumt[o_topic] -= 1
        self.sumd[m] -= 1

        #compute the probabilty p_z for the w
        left = (self.nd[m, :] + self.alpha) /\
               (self.sumd[m] + self.alpha * self.K)
        right = (self.nw[w, :] + self.beta) /\
                (self.sumt + self.beta * self.V)
        p_k = left * right
        "normalize p_k"
        p_k = p_k / sum(p_k)

        new_topic = sample_multinormial(p_k)

        self.nw[w][new_topic] += 1
        self.nd[m][new_topic] += 1
        self.sumt[new_topic] += 1
        self.sumd[m] += 1
        self.z[m][n] = new_topic

    def get_phi(self):
        for i in xrange(self.K):
            if sum(self.nw[:, i]) == .0:
                self.phi[i, :] = np.zeros((1, len(self.phi[i, :])))
            else:
                self.phi[i, :] = self.nw[:, i] / sum(self.nw[:, i])

    def get_theta(self):
        for i in xrange(self.M):
            self.theta[i, :] = self.nd[i, :] / self.sumd[i]

    def estimate(self):
        "Apply Gibbs Samping to estimate the parameters"
        #randomly assign topic to each word in documents
        p_z = [1.0 / self.K] * self.K
        for m in xrange(len(self.z)):
            for n, w in enumerate(self.doc_matrix[m]):
                topic = sample_multinormial(p_z)

                self.z[m][n] = topic
                self.nw[w][topic] += 1
                self.nd[m][topic] += 1
                self.sumt[topic] += 1
                self.sumd[m] += 1
        #Gibbs sampling
        for iter in xrange(self.iternum):
            print "%d iteration\n" % (iter)
            for m in xrange(len(self.z)):
                for n, w in enumerate(self.doc_matrix[m]):
                    self.gib_sample(m, n)

        #Get the phi and theta
        self.get_phi()
        self.get_theta()

    def out_put_result(self, out_d):
        out_matrix = out_d + os.sep + "word_topic_count.txt"
        out_theta = out_d + os.sep + "theta.txt"
        out_phi = out_d + os.sep + "phi.txt"

        np.save(out_matrix, self.nw)
        np.save(out_theta, self.theta)
        np.save(out_phi, self.phi)


def main():
    ap = args.get_parser()
    ap.add_argument('--df', help='The datafile to train')
    ap.add_argument('--od', help='the directory to store output')
    arg = ap.parse_args()

    lda = Lda()
    lda.doc_process(arg.df)
    lda.out_put_wordmap(arg.od)
    lda.estimate()
    lda.out_put_result(arg.od)


if __name__ == "__main__":
    main()
