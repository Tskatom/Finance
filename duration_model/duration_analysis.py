#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = "Wei Wang"
__email__ = "tskatom@vt.edu"


from datetime import datetime, timedelta
import math
import copy

target_indices = ['MEXBOL', 'MERVAL', 'CRSMBCT', 'COLCAP', 'IBOV', 'IBVC', 'IGBVL', 'BVPSBVPS', 'CHILE65']
CURRENCY_LIST = ['USDARS', 'USDBRL', 'USDCLP', 'USDCOP', 'USDCRC', 'USDMXN', 'USDPEN']


def load(data_file):
    sequences = {}

    with open(data_file, "r") as r:
        data = r.readlines()[1:]
        for d in data:
            infos = d.strip().split("|")
            stock = infos[1]
            post_date = infos[2]
            z30 = float(infos[3])
            z90 = float(infos[4])

            if stock not in sequences:
                sequences[stock] = {}
            sequences[stock][post_date] = [z30, z90]
    return sequences


def get_pastdate(event_date, duration=6):
    event_date = datetime.strptime(event_date, '%Y-%m-%d')
    days = [datetime.strftime(event_date - timedelta(days=(i + 1)),
                              '%Y-%m-%d') for i in xrange(duration)]
    return days


def get_gsr(sequences):
    gsr_events = {}
    for index in target_indices:
        index_data = sequences[index]
        gsr_events[index] = {}
        for k, v in index_data.items():
            if math.fabs(v[0]) > 2 or math.fabs(v[1]) > 2:
                gsr_events[index][k] = v
    return gsr_events


#finding the most frequent events before sigmaevent
def analyze(sequence, gsr_events):
    frequency = {}
    for index in gsr_events:
        index_data = gsr_events[index]
        frequency[index] = {}
        for post_date, zs in index_data.items():
            days = get_pastdate(post_date)
            for day in days:
                for stock, values in sequence.items():
                    if stock not in frequency[index]:
                        frequency[index][stock] = {"z30": {}, "z90": {}}
                    #get indicate day's z30 and z90
                    if day in values:
                        z30c = frequency[index][stock]["z30"].setdefault(values[day][0], 0)
                        z90c = frequency[index][stock]["z90"].setdefault(values[day][1], 0)

                        frequency[index][stock]["z30"][values[day][0]] = z30c + 1
                        frequency[index][stock]["z90"][values[day][1]] = z90c + 1

    #normalize the frequency
    for index in frequency:
        for indep_stock in frequency[index]:
            for zs in frequency[index][indep_stock]:
                sumval = sum(frequency[index][indep_stock][zs].values())
                for k, v in frequency[index][indep_stock][zs].items():
                    temp = round(float(v) / sumval, 2)
                    if temp < 0.3:
                        del(frequency[index][indep_stock][zs][k])
                    else:
                        frequency[index][indep_stock][zs][k] = temp

    return frequency, gsr_events


def check_days_duration(target_day, days, duration=6):
    target_day = datetime.strptime(target_day, "%Y-%m-%d")
    days = [datetime.strptime(day, "%Y-%m-%d") for day in days]
    for day in days:
        diff = (day - target_day).days
        if diff > 0 and diff <= duration:
            return True

    return False


#extract the high confience event before sigmaevent
def analyze2(sequence, gsr_events):
    "construct the event matrix"
    event_matrix = {}
    for stock, zs in sequence.items():
        event_matrix[stock] = {"z30": {}, "z90": {}}
        for day, scores in zs.items():
            z30 = scores[0]
            z90 = scores[1]
            if z30 not in event_matrix[stock]["z30"]:
                event_matrix[stock]["z30"][z30] = {}
            if z90 not in event_matrix[stock]["z90"]:
                event_matrix[stock]["z90"][z90] = {}
            event_matrix[stock]["z30"][z30].setdefault(day, 0)
            event_matrix[stock]["z90"][z90].setdefault(day, 0)

    "iterate the gsr events"
    evaluate_matrix = {}
    for g_index, g_events in gsr_events.items():
        e_days = g_events.keys()
        evaluate_matrix[g_index] = copy.deepcopy(event_matrix)
        print "start1"
        for in_index in evaluate_matrix[g_index]:
            print "start2"
            for zs in evaluate_matrix[g_index][in_index]:
                for event_type in evaluate_matrix[g_index][in_index][zs]:
                    for day in evaluate_matrix[g_index][in_index][zs][event_type]:
                        #check if sigma event happening in next seven days
                        if check_days_duration(day, e_days):
                            evaluate_matrix[g_index][in_index][zs][event_type][day] = 1

    return evaluate_matrix


#output the performance of the analysis
def output_result(eva_matrix, gsr_events, out_file, threshold=0.4):
    rule = {}
    for index in eva_matrix:
        rule[index] = {}
        for in_index in eva_matrix[index]:
            rule[index][in_index] = {}
            for zs in eva_matrix[index][in_index]:
                rule[index][in_index][zs] = {}
                for event_type, summary in eva_matrix[index][in_index][zs].items():
                    pro = round(float(sum(summary.values())) / len(summary), 2)
                    if pro >= threshold:
                        print index, in_index, zs, event_type, pro, len(summary), int(sum(summary.values())), len(gsr_events[index])
                        rule[index][in_index][zs][event_type] = .6
                if len(rule[index][in_index][zs]) == 0:
                    del rule[index][in_index][zs]
            if len(rule[index][in_index]) == 0:
                del rule[index][in_index]

    #add currency rules
    for currency in CURRENCY_LIST:
        rule[currency] = {currency: {'z30': {-3.0: 0.5, 3.0: 0.5}, 'z90': {-3.0: 0.5, 3.0: 0.5}}}

    rule = {"version": "1", "rules": rule}

    with open(out_file, "w") as w:
        w.write(str(rule))


if __name__ == "__main__":
    pass
