#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = "Wei Wang"
__email__ = "tskatom@vt.edu"

import sys
import os
from datetime import datetime
import hashlib
import json
import boto


def main():
    r_file = sys.argv[1]
    conn = boto.connect_sdb()
    t_domain = conn.get_domain('bloomberg_prices')
    with open(r_file, 'r') as f_r:
        for line in f_r:
            line = line.replace("\r", "").replace("\n", "").split(",")
            name = line[0].split(" ")[0]
            if line[0].split(" ")[1] == "Index":
                ty = "stock"
            else:
                ty = "currency"
            da = datetime.strptime(line[1], "%m/%d/%Y").strftime("%Y-%m-%d")
            last_price = float(line[2])
            previous_last_price = float(line[3])

            raw_data = {"currentValue": last_price, "date": '2013-04-10T16:00:01+00:00', "feed": "bloomberg_terminal", "name": name, "originalUpdateTime": '2013-04-10T16:00:01+00:00', "previousCloseValue": previous_last_price, "queryTime": '2013-04-10T19:40:53.420752+00:00', "type":ty}
            embers_id = hashlib.sha1(json.dumps(raw_data)).hexdigest()
            raw_data["embersId"] = embers_id
            t_domain.put_attributes(embers_id, raw_data)
            print raw_data


if __name__ == "__main__":
    main()
