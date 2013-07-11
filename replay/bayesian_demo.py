#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = "Wei Wang"
__email__ = "tskatom@vt.edu"

import sys
from subprocess import call
import json


def main():
    trend_file = sys.argv[1]
    audit_trail = sys.argv[2]
    print trend_file
    print audit_trail
    warns = [json.loads(l) for l in open(audit_trail)]
    warn_pairs = [(w["eventDate"], w["population"]) for w in warns]
    for wp in warn_pairs:
        command = "bash ./wrapper_bayesian_model.sh %s %s %s " % (wp[0], wp[1], trend_file)
        call(command, shell=True)


if __name__ == "__main__":
    main()
