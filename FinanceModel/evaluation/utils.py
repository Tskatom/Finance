#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# vim: ts=4 sts=4 sw=4 tw=79 sta et
"""%prog 
Utils for Civil Unrest module.
"""

__author__ = 'Rupen'
__email__ = 'rupen.paul@cs.vt.edu'

TIME_FORMAT = "%Y-%m-%dT%H:%M:%S+00:00"
TIME_ISOFORMAT = "%Y-%m-%dT%H:%M:%S"
TIME_GSRFORMAT = "%Y-%m-%d"
TIME_GSRFORMAT1 = "%Y-%d-%m"

from math import atan, pi
from dateutil import parser
from datetime import datetime
from time import strptime
import chardet
from munkres import Munkres, make_cost_matrix

MUNKR = Munkres()

CIVIL_UNREST_OSI_EVENT_CODES = {'011':"Employment and Wages", '012':"Housing",
                                 '013':"Energy and Resources", 
                                 '014':"Other Economic Policies",
                                 '015':"Other Government Policies",
                                 '016':"Other", '017': "Unspecified"
                                }

POPULATION_TYPE_CODES   = {'01':"General Population", '02':"Business", 
                           '03':"Ethinic", '04':"Legal", '05':"Education",
                           '06':"Religious", '07':"Medical", '08':"Media",
                           '09':"Labor", '10':"Refugees or Displaced",
                           '11':"Agricultural"
                          }

CIVIL_UNREST_OSI_CODES = ['011','012','013','014','015','016','017']

VIOLENCE_OSI_CODES     = {'1': "Non-Violence", '2': "Violence"}

LANG_CODES = {"en":"english","es":"spanish","pt":"portuguese"}

STAGE_CODES = {"pr":"pre-protest", "pl":"planned-protest",
               "ac":"actual-protest", "po":"post-protest"}

angle_in_deg = lambda x : atan(x) * (180 / pi)
str_dt = lambda x : parser.parse(x)
 
def get_hash_key(x):
    import hashlib
    return hashlib.sha1(str(x)).hexdigest()

def iso_str_dt(date_str):
    return datetime(*strptime(date_str,TIME_ISOFORMAT)[0:6]) 

def warning_str_dt(date_str):
    return iso_str_dt(date_str)

def gsrevent_str_dt(date_str):
    return datetime(*strptime(date_str,TIME_GSRFORMAT)[0:6])

def timedelta_days_hours_minutes(td):
    return td.days, td.seconds//float(3600), (td.seconds//float(60))%60 

def timedelta_days(td, round_off=True):
    d,h,m = timedelta_days_hours_minutes(td) 
    t = d + float(h/24.0) + float(m/float(60*24))
    if round_off:
        return round(t)
    else:
        return t

def format_str_unicode(s):
    if isinstance(s,basestring):
        if not isinstance(s,unicode):
            try:
                s = s.decode("utf8")
            except UnicodeDecodeError:
                s = unicode(s,chardet.detect(s)['encoding'])
    text = u' '.join(s.strip().lower().split())
    return u' '.join(s.splitlines())

def calc_months_from_time_range(start, end):
    start_month= start.month
    end_months=(end.year - start.year)*12 + (end.month + 1)
    return [datetime(year=yr, month=mn, day=1) for (yr, mn) in \
            (((m - 1) / 12 + start.year, (m - 1) % 12 + 1) for m in \
                                               range(start_month, end_months)) \
           ]

def jaccard(set1, set2):
    n = len(set1.intersection(set2))
    return n / float(len(set1) + len(set2) - n)

def group_by(data, key=None, value=None):
    grps = {}
    for d in data:
        k = key(d)
        if k is not None:
            if k not in grps:
                grps[k] = []
            if value is None:
                grps[k].append(d)
            else:
                grps[k].append(value(d))
    return grps

def filter_by_date(start, end, data, key=None):
    fdata = []
    for d in data:
        curr = key(d)
        if curr >= start and curr <= end:
            fdata.append(d)
    return fdata

def load_gsr_warnings(filename="./data/all_gsr_warnings.txt", \
                      eventType=None):
    import json
    gsr_events = [json.loads(e) for e in open(filename,'r').readlines()]
    if eventType is not None:
        gsr_events = group_by(gsr_events,key=lambda e : e['eventId'][:2])
        try:
            gsr_events = gsr_events[eventType]
        except KeyError:
            print "eventType %s not found in gsr" % (eventType)
            return []

    for g in gsr_events:
        g['date'] = gsrevent_str_dt(g['date']).strftime(TIME_ISOFORMAT)
        g['eventDate'] = gsrevent_str_dt(g['eventDate']).strftime(TIME_ISOFORMAT)
        g['location'] = [l.strip() for l in g['location'].split(',')]
        while len(g['location']) < 3:
            g['location'].append("-")
    return gsr_events 

def load_warnings(filename="./data/warning.txt"):
    import json
    warnings = [json.loads(e) for e in open(filename,'r').readlines()]

    for g in warnings:
        g['date'] = gsrevent_str_dt(g['date']).strftime(TIME_ISOFORMAT)
        g['eventDate'] = gsrevent_str_dt(g['eventDate']).strftime(TIME_ISOFORMAT)
        g['location'] = [l.strip() for l in g['location'].split(',')]
        while len(g['location']) < 3:
            g['location'].append("-")
    return warnings 

def do_hungarian_assignment(dict_a, dict_b, cost_func, yield_condt, cost_matrix_func=None):
    for ka  in dict_a.keys():
        if ka in dict_b:
            ia  = dict_a[ka]
            ib  = dict_b[ka]
            mat = [ [cost_func(a,b) for b in ib] for a in ia]
            #max similarity calculation
            c_mat = None
            if cost_matrix_func is not None:
                c_mat = make_cost_matrix(mat,cost_matrix_func)
            else:
                c_mat = mat
            indexes = MUNKR.compute(c_mat)
            for row, col in indexes:
                # yield only if condition satisfied
                if yield_condt(mat[row][col]):
                    #print '(%d, %d) -> %d' % (row, col, mat[row][col])
                    yield ia[row], ib[col]

def fileobj(strobj, mode="r"):
    """@todo: Docstring for fileobj

    :param strobj: either a string or a file object, if the former open a file
                   return if the latter pass it through
    :returns: a file object

    """
    if isinstance(strobj, basestring):
        return open(strobj, mode)
    else:
        assert(hasattr(strobj, 'read'))
        return strobj

def get_freqDist_chains(chains):
    freq = {}
    for id, c in chains.iteritems():
        count = len(c)
        if str(count) in freq:
            freq[str(count)] += 1
        else:
            freq[str(count)] = 1
    print freq


def print_bins(bins):
    i = 1
    for bin in bins:
        print "%s Start: %s\nEnd: %s\n count: %s\n" % (i,bin['start'].strftime(TIME_FORMAT),
                                                       bin['end'].strftime(TIME_FORMAT),
                                                       len(bin['tweets']))
        i += 1
