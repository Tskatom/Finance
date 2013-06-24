#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = "Wei Wang"
__email__ = "tskatom@vt.edu"


import codecs
import os
import glob
from etool import args
import unicodedata

index = 0


def normalize_str(s):
    if isinstance(s, str):
        s = s.decode('utf-8')

    s = unicodedata.normalize('NFKD', s)
    return s.encode('ASCII', 'ignore').lower()


def transfer2ldaf(doc_file, out_f):
    global index
    with codecs.open(doc_file) as doc_f:
        for line in doc_f:
            news = {}
            news['title'] = normalize_str(eval(line)['title'])
            news['postTime'] = eval(line)['postTime']
            news['index'] = index
            index += 1
            out_f.write(str(news) + "\n")


def process(file_dir, out_f):
    os.chdir(file_dir)
    for file in glob.glob('*.txt'):
        f = file_dir + os.sep + file
        transfer2ldaf(f, out_f)


def main():

    ap = args.get_parser()
    ap.add_argument('--fd', help='the directory of the files needed to processed')
    ap.add_argument('--f', help='the file need to be prcessed')
    ap.add_argument('--o', help='the output file name')

    arg = ap.parse_args()
    assert arg.o, 'Need a output file'

    with codecs.open(arg.o, encoding='ascii', mode="w") as out_f:
        if arg.fd:
            process(arg.fd, out_f)
        elif arg.f:
            transfer2ldaf(arg.f, out_f)
        else:
            pass


if __name__ == "__main__":
    main()
