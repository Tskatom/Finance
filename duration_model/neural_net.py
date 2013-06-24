#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = "Wei Wang"
__email__ = "tskatom@vt.edu"


from pybrain.structure import FeedForwardNetwork
from pybrain.structure import LinearLayer, SigmoidLayer, BiasUnit
from pybrain.datasets import SupervisedDataSet
from pybrain.supervised.trainers import BackpropTrainer
from pybrain.structure import FullConnection
from pybrain.datasets import ClassificationDataSet
import math
import numpy as np
from datetime import datetime
import sys


VAR = ['AEX', 'AS51', 'BVPSBVPS', 'CAC', 'CCMP', 'CHILE65', 'COLCAP',
       'CRSMBCT', 'DAX', 'FTSEMIB', 'HSI', 'IBEX', 'IBOV', 'IBVC',
       'IGBVL', 'INDU', 'MERVAL', 'MEXBOL', 'NKY', 'OMX', 'SMI',
       'SPTSX', 'SX5E', 'UKX', 'USDARS', 'USDBRL', 'USDCLP',
       'USDCRC', 'USDCOP', 'USDMXN', 'USDPEN']

TARGET = ['BVPSBVPS', 'CHILE65', 'COLCAP', 'CRSMBCT', 'IBOV', 'IBVC',
          'IGBVL', 'MERVAL', 'MEXBOL', 'USDARS', 'USDBRL', 'USDCLP', 'USDCOP',
          'USDCRC', 'USDMXN', 'USDPEN']


def sigmoid(z):
    return 1. / (1 + math.e ** (-1 * z))


#transfer the zscore to [-4,-3, -2, -1, 0, 1, 2, 3,4]
def transfer_f(zscore):
    if zscore > 0:
        zscore = math.floor(zscore)
        if zscore >= 4.0:
            zscore = 4
    else:
        zscore = math.ceil(zscore)
        if zscore <= -4.0:
            zscore = -4

    #merge -0 and 0 into one value
    if zscore < 0.05 and zscore > -.05:
        zscore = 0

    return int(zscore)


#ignore the difference between negative and positive
def transfer_f2(zscore):
    if zscore > 0:
        zscore = math.floor(zscore)
        if zscore >= 3:
            zscore = 3
    else:
        zscore = math.ceil(zscore)
        if zscore <= -3:
            zscore = -3

    z = math.fabs(int(zscore))
    if z >= 2:
        return 1
    else:
        return 0


#filter data
def filt(enriched_file, s_date, e_date, symbols):
    messages = [eval(m) for m in open(enriched_file, "r").readlines()]
    filted = [m for m in messages if m["name"] in symbols and m["postDate"] >= s_date and m["postDate"] <= e_date]
    return filted


def check_days_duration(target_day, days, duration=5):
    target_day = datetime.strptime(target_day, "%Y-%m-%d")
    days = [datetime.strptime(day, "%Y-%m-%d") for day in days]
    for day in days:
        diff = (day - target_day).days
        if diff > 0 and diff <= duration:
            return True
    return False


def initiate_input(enriched_file, out_file, s_date, e_date):
    #the order of input

    en_messages = {}
    gsr_events = {}
    with open(enriched_file, "r") as r:
        for line in r.readlines():
            m = eval(line.strip())
            p_date = m['postDate']
            name = m['name']
            if p_date not in en_messages:
                en_messages[p_date] = {}
            en_messages[p_date][name] = m

            #extract gsr events
            if name in TARGET:
                z30 = float(m["zscore30"])
                z90 = float(m["zscore90"])
                if math.fabs(z30) >= 4 or math.fabs(z90) >= 3:
                    if z30 > 0.05:
                        flag = 1
                    else:
                        flag = -1
                    if name not in gsr_events:
                        gsr_events[name] = {}
                    gsr_events[name][p_date] = flag

    gsr_dates = {}
    for index in gsr_events:
        gsr_dates[index] = sorted(gsr_events[index].keys())

    #read the data in order
    dates = sorted(en_messages.keys())
    dates = [d for d in dates if d >= s_date and d <= e_date]
    #write result to file
    w_out = open(out_file, "w")

    for d in dates[:]:
        vars = []
        targ = []
        #get variables
        for v in VAR:
            if v in en_messages[d]:
                z30 = transfer_f2(float(en_messages[d][v]["zscore30"]))
                z90 = transfer_f2(float(en_messages[d][v]["zscore90"]))
                vars.append(str(z30))
            else:
                vars.append(str(0))

        #get targets if events happen in future 6 days, then set value 1 else 0
        for v in TARGET:
            if v in gsr_dates:
                #check the day duration
                e_flag = check_days_duration(d, gsr_dates[v])
                if e_flag:
                    targ.append('1')
                else:
                    targ.append('0')
            else:
                targ.append('0')

        var_str = ' '.join(vars)
        targ_str = ' '.join(targ)
        w_out.write("%s|%s|%s\n" % (d, var_str, targ_str))

    w_out.flush()
    w_out.close()
    return en_messages, gsr_events, gsr_dates


def load_input(in_file):
    with open(in_file, "r") as r:
        first_line = r.readline()
        vars = np.fromstring(first_line.split("|")[1], sep=' ')
        targs = np.fromstring(first_line.split("|")[2], sep=' ')

        for line in r:
            infos = line.split("|")
            vars = np.vstack([vars, np.fromstring(infos[1], sep=' ')])
            targs = np.vstack([targs, np.fromstring(infos[2], sep=' ')])

        return vars, targs


def training(vars, targs,  max_iter):
    #construct the network
    net = FeedForwardNetwork()
    in_num = vars.shape[1]
    hidd_num = in_num + in_num / 2
    out_num = 2

    inLayer = LinearLayer(in_num)
    hiddenLayer = SigmoidLayer(hidd_num)
    outLayer = SigmoidLayer(out_num)
    bias = BiasUnit('bias')

    #add module into network
    net.addInputModule(inLayer)
    net.addModule(bias)
    net.addModule(hiddenLayer)
    net.addOutputModule(outLayer)

    #construct the Connection
    in2hidd = FullConnection(inLayer, hiddenLayer)
    b2hidd = FullConnection(bias, hiddenLayer)
    hidd2out = FullConnection(hiddenLayer, outLayer)
    b2out = FullConnection(bias, outLayer)

    net.addConnection(in2hidd)
    net.addConnection(b2hidd)
    net.addConnection(hidd2out)
    net.addConnection(b2out)

    net.sortModules()

    #handle the input data
    trnData = ClassificationDataSet(in_num, 1, nb_classes=2)
    for i in xrange(vars.shape[0]):
        try:
            trnData.addSample(vars[i], targs[i][3])
        except:
            print "i=%d" % i
            sys.exit(0)

    trnData._convertToOneOfMany()

    trainer = BackpropTrainer(net, dataset=trnData, momentum=0.1, verbose=True, weightdecay=0.01)

    #train the model
    for i in range(max_iter):
        print i
        trainer.train()

    return net, trainer, trnData


def comovement(vars, targs, threa_shold):
    #find the comovement of the events
    co_move = []
    for v_col in range(vars.shape[1]):
        co_move.append([])
        #get all the days' event happening
        indices = np.where(vars[:, v_col] > 0)[0]
        day_num = indices.shape[0]
        #get days for targets
        for t_col in range(targs.shape[1]):
            event_num = np.where(targs[indices, t_col] > 0)[0].shape[0]
            ratio = 1.0 * event_num / day_num
            print "%s|%s|%d|%d" % (VAR[v_col], TARGET[t_col], event_num, day_num)
            co_move[v_col].append(round(ratio, 2))

    ratios = np.array(co_move)
    rules = {}
    rules['version'] = "2"
    rules['rules'] = {}
    pairs = {}
    #extract the strong correlation
    for v_index in range(ratios.shape[0]):
        v_stock = VAR[v_index]
        corr_in = [i for i in np.where(ratios[v_index] >= threa_shold)[0]]
        print "%s:" % v_stock
        for m in corr_in:
            t_stock = TARGET[m]
            cor = ratios[v_index, m]
            print "\t%s:%5.2f" % (t_stock, cor)
            if t_stock not in pairs:
                pairs[t_stock] = set()
            pairs[t_stock].add(v_stock)

    #construct rules according to the pairs
    for t_stock in pairs:
        rules['rules'][t_stock] = {}
        if t_stock == 'IBVC':
            rules['rules'][t_stock][t_stock] = {"z30": {3.0: 0.5, -3.0: 0.5}, "z90": {3.0: 0.5, -3.0: 0.5}}
        else:
            for v_stock in pairs[t_stock]:
                rules['rules'][t_stock][v_stock] = {"z30": {3.0: 0.5, -3.0: 0.5}, "z90": {3.0: 0.5, -3.0: 0.5}}

    with open('rule3.txt', 'w') as w:
        w.write(str(rules))
    return rules


def test():
    ds = SupervisedDataSet(2, 1)

    net = FeedForwardNetwork()
    inLayer = LinearLayer(2)
    hiddenLayer = SigmoidLayer(5)
    outLayer = SigmoidLayer(1)
    bias = BiasUnit('bias')

    net.addInputModule(inLayer)
    net.addModule(hiddenLayer)
    net.addOutputModule(outLayer)
    net.addModule(bias)

    in_to_hidd = FullConnection(inLayer, hiddenLayer)
    hi_to_out = FullConnection(hiddenLayer, outLayer)
    bias_to_hidd = FullConnection(bias, hiddenLayer)
    bias_to_out = FullConnection(bias, outLayer)

    net.addConnection(in_to_hidd)
    net.addConnection(hi_to_out)
    net.addConnection(bias_to_hidd)
    net.addConnection(bias_to_out)

    net.sortModules()
    pairs = [(0, 0, 0), (1, 1, 1), (0, 1, 0), (1, 0, 0)]
    for x in range(40):
        for i in range(4):
            p = pairs[i]
            ds.addSample((p[0], p[1]), p[2])

    trainer = BackpropTrainer(net, ds)

    for i in range(400):
        trainer.train()
        print "Epoch : %4d" % i

    print net.activate((0, 0))
    return net

if __name__ == "__main__":
    pass
