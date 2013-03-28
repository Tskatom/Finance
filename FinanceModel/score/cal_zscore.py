#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = "Wei Wang"
__email__ = "tskatom@vt.edu"

import sys
import os
import math
import numpy as np
import calculator as cl

bvpsbvps = []
bgfg = []
egin = []


def read_csv(csv_name):
    with open(csv_name,"r") as csv_r:
        lines = csv_r.readlines()
        for line in lines:
            infos = line.replace("\n","").split(",")
            if infos[0] == "BVPSBVPS":
                bvpsbvps.append(infos)
            elif infos[0] == "BGFG PP Equity":
                bgfg.append(infos)
            elif infos[0] == "EGIN PP Equity":
                egin.append(infos)


def cal_zscore():
    # compute the return
    for data in [bvpsbvps,bgfg,egin]:
        data[0].append(.0)
        for i in range(1, len(data)):
            data[i].append(round(float(data[i][2])-float(data[i-1][2]),4))


    #compute the zscore,initiate the zscore30 and zscore90
    for data in [bvpsbvps,bgfg,egin]:
        for d in data:
            d.extend([.0,.0])
    

    #calcuate z30
    for data in [bvpsbvps, bgfg, egin]:
        for i in range(30, len(data)):
            d_list = [w[3] for w in data[i-30:i]]
            z30 = cl.calZscore(d_list,data[i][3])
            #mean = np.mean(d_list)
            #std = np.std(d_list)
            #if std != .0:
            #    z30 = (data[i][3] - mean) / std
            #else:
            #    z30 = .0
            data[i][4] = round(z30,4)

    #calcuate z90
    for data in [bvpsbvps, bgfg, egin]:
        for d in data:
            for i in range(90,len(data)):
                d_list = [w[3] for w in data[i-90:i]]
                z90 = cl.calZscore(d_list,data[i][3])
                #mean = np.mean(d_list)
                #std = np.std(d_list)
                #if std != .0:
                #   z90 = (data[i][3] - mean) / std
                #else:
                #    z90 = .0
                data[i][5] = round(z90,4)


def write2file():
    with open('/home/vic/workspace/data/Members/e_bvpsbvps.csv',"w") as w:
        w.write("Ticker,date,price,return,z30,z90\n")
        for data in [bvpsbvps,bgfg,egin]:
            for d in data:
                w.write(d[0]+","+d[1]+","+d[2]+","+str(d[3])+","+str(d[4])+","+str(d[5])+"\n")

    


def main():
    csv_file = '/home/vic/workspace/data/Members/BVPSBVPS.csv'
    read_csv(csv_file)
    cal_zscore()
    write2file()

if __name__ == "__main__":
    main()

