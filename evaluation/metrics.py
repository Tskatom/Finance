#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# vim: ts=4 sts=4 sw=4 tw=79 sta et
"""%prog 
Run historical twitter feeds for Civil Unrest modeling using
spatial scan.
"""

__author__ = 'Rupen'
__email__ = 'rupen.paul@cs.vt.edu'

import os
import sys
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if not path in sys.path:
    sys.path.insert(1, path)
del path

from utils import iso_str_dt, timedelta_days

MAX_ALLOWED_INTERVAL_DAYS = 7

class CUMetrics():
    def __init__(self):
        pass

    @staticmethod
    def calc_precision_recall(tot_matches, tot_warnings, tot_gsr_events):
        prec,recall = [0.0] * 2
        if tot_warnings > 0:
            prec = round(float(tot_matches) / tot_warnings,2)
        if tot_gsr_events > 0:
            recall = round(float(tot_matches) / tot_gsr_events,2)
        return prec,recall

    @staticmethod
    def calc_event_date_delta_days(warning, gsr_event):
        a = iso_str_dt(warning['eventDate'])
        b = iso_str_dt(gsr_event['eventDate'])
        if a > b:
            return timedelta_days(a - b)
        else:
            return timedelta_days(b - a)

    @staticmethod
    def calc_quality(warning, gsr_event):
        x1,x2,x3,event_score,date_score, co, st, ci = [0.0] * 8
        
        "compute event type score"
        w_event = warning["eventType"]
        g_event = gsr_event["eventId"]
        if w_event[:2] == g_event[:2]:
            x1 = 1.0
        if w_event[2] == g_event[2]:
            x2 = 1.0
        if w_event[3] == g_event[3]:
            x3 = 1.0
        event_score = (x1 + x1*x2 + x1*x2*x3)/3.0
        
        "compute date score"
        delta_days = CUMetrics.calc_event_date_delta_days(warning, gsr_event)
        date_score = 1.0 - (float(min(delta_days,MAX_ALLOWED_INTERVAL_DAYS)) \
                         / MAX_ALLOWED_INTERVAL_DAYS)
        
        "compuate location score"
        if gsr_event['location'][0] == '-' or \
            warning['location'][0] == gsr_event['location'][0]:
            co = 1.0
        if gsr_event['location'][1] == '-' or \
            warning['location'][1] == gsr_event['location'][1]:
            st = 1.0
        if gsr_event['location'][2] == '-' or \
            warning['location'][2] == gsr_event['location'][2]:
            ci = 1.0
        location_score = (co + co * st + co * st * ci)/3.0
        return [event_score,date_score, location_score ]

    @staticmethod
    def calc_lead_time(warning, gsr_event):
        warning_date   = iso_str_dt(warning['date'])
        gsr_event_date = iso_str_dt(gsr_event['date'])
        if gsr_event_date <= warning_date:
            #TODO it would be better if gsr timestamp included hr:min atleast?
            return 0
        else:
            #diff days
            return timedelta_days(gsr_event_date - warning_date)

    @staticmethod
    def calc_probability(o,warning):
        diff = o - warning['confidence']
        return 1 - (diff * diff)

    @staticmethod
    def calc_mean_scores(matches,matched_warning,warnings_mo_co_list):
        count = 0
        avgQualScore = 0
        avgLeadScore = 0
        avgProbScore = 0
        for m in matches:
            avgQualScore += sum(CUMetrics.calc_quality(m[0],m[1]))
            avgLeadScore += CUMetrics.calc_lead_time(m[0],m[1])
            count += 1

        "Compute mean probability"
        ct = 0
        for w in warnings_mo_co_list:
            if w['embersId'] in matched_warning:
                o = 1
            else:
                o = 0
            avgProbScore += CUMetrics.calc_probability(o,w)
            ct += 1
        
        if ct == 0:
            return 0.0,0.0,0.0
        else:
            avgProbScore = round((avgProbScore/float(ct)) * 100.0) / 100.0
        
        if count == 0:
            return 0.0,0.0,avgProbScore
            
        avgQualScore = round((avgQualScore/float(count)) * 100.0) / 100.0
        avgLeadScore = round((avgLeadScore/float(count)) * 100.0) / 100.0

        return avgQualScore, avgLeadScore, avgProbScore

