#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = "Wei Wang"
__email__ = "tskatom@vt.edu"


import json
import os
import glob
import re
import nltk
from nltk.stem.wordnet import WordNetLemmatizer

RULE = "(Brazil|braizl|Brazilian|BRL|SELIC|USDBRL|IBOV)"


def read_words(file_dir):
    os.chdir(file_dir)
    pattern = re.compile(RULE, re.I)
    matched = []
    for file in glob.glob('*News.txt'):
        f = file_dir + file
        news = json.load(open(f, "r"), encoding='latin-1')
        for n in news:
            content = n["content"]
            finds = pattern.findall(content)
            if finds:
                matched.append(content)

    for file in glob.glob('or*.txt'):
        f = open(file_dir + file, "r")
        for line in f.readlines():
            news = json.loads(line, encoding='latin-1')
            if "content" in news:
                content = news["content"]
                finds = pattern.findall(content)
                if finds:
                    matched.append(content)
        f.close()

    return matched


def check_stopwords(w, lan='english'):
    if w.lower() in nltk.corpus.stopwords.words(lan):
        return True
    else:
        return False


def check_numsandspe(w):
    if re.search("[\d,.():;@&\[\]\'\"-\-\*%]", w):
        return True
    else:
        return False


def get_words(content):
    lemmtizer = WordNetLemmatizer()
    words = nltk.word_tokenize(content)
    words = [lemmtizer.lemmatize(w) for w in words if (not check_stopwords(w) and not check_numsandspe(w))]
    return words


def main():
    file_dir = '/media/0012-D687/Wei/news/'
    news = read_words(file_dir)
    all_words = []

    for n in news:
        words = get_words(n)
        all_words.extend(words)

    wordFreq = nltk.FreqDist(all_words)
    with open(file_dir + "word_list.txt", "w") as w:
        for key in wordFreq.keys():
            v = wordFreq.get(key)
            try:
                w.write(key + "\t" + str(v) + "\n")
            except Exception as e:
                print e
                continue
    print wordFreq


if __name__ == "__main__":
    main()
