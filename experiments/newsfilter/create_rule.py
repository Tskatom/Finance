#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = "Wei Wang"
__email__ = "tskatom@vt.edu"


from etool import args
import os
import glob
import json


COUNTRY_MARKET = {"MERVAL": "Argentina", "USDARS": "Argentina", "IBOV": "Brazil", "USDBRL": "Brazil",
                  "CHILE65": "Chile", "USDCLP": "Chile", "COLCAP": "Colombia", "USDCOP": "Colombia",
                  "CRSMBCT": "Costa Rica", "USDCRC": "Costa Rica", "MEXBOL": "Mexico", "USDMXN": "Mexico",
                  "BVPSBVPS": "Panama", "IGBVL": "Peru", "USDPEN": "Peru", "IBVC": "Venezuela"}


def main():
    ap = args.get_parser()
    ap.add_argument("--dir", type=str, help="directory of company member")
    ap.add_argument("--o", type=str, help="the directory of output ")
    arg = ap.parse_args()

    assert arg.dir, 'Need a dir to explor'
    rules = {}
    print arg
    os.chdir(arg.dir)
    for f in glob.glob('*.csv'):
        stock = f.split(".")[0].split("_")[1]
        country = COUNTRY_MARKET[stock]
        rules[country] = []
        with open(f, 'r') as f_r:
            i = 0
            for line in f_r:
                i += 1
                if i >= 2:
                    l = line.strip().split(",")
                    company = l[2]
                    if company == "":
                        continue
                    tmp = company.split(" ")
                    if len(tmp) > 1:
                        tmp = tmp[0:len(tmp) - 1]

                    if company is not None:
                        rules[country].append(company.strip())

        rules[country].append(country)

    with open(arg.o, "w") as o_w:
        o_w.write(json.dumps(rules))


if __name__ == "__main__":
    main()
