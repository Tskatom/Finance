#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# vim: ts=4 sts=4 sw=4 tw=79 sta et
"""%prog 
Run historical twitter feeds for Civil Unrest modeling using
spatial scan.
"""

__author__ = 'Rupen'
__email__ = 'rupen.paul@cs.vt.edu'

from metrics import CUMetrics as mt
from utils import filter_by_date, iso_str_dt, \
                  group_by, do_hungarian_assignment

MAX_SIMILARITY_SCORE = 4
LEAD_TIME_THRESHOLD  = 0
SCORING_INTERVAL     = 7

class MatchingGlobalOptimum():
    def __init__(self, interval=SCORING_INTERVAL, \
                 similarity_fn=None, cost_matrix_fn=None, \
                 yield_condt=None, similarity_weights=None):
        from utils import do_hungarian_assignment
        self.scoring_interval = interval
        #TODO sum of similarity weights
        self.max_similarity_score  = MAX_SIMILARITY_SCORE
        self.sim_fn = similarity_fn
        self.weights = similarity_weights
        self.cost_matrix_fn = cost_matrix_fn
        self.yield_condt = yield_condt

        if self.weights is None:
            self.weights = [4/3.0,4/3.0,4/3.0]

        if self.sim_fn is None:
            def sim_fn(x,y):
                if mt.calc_lead_time(x,y) > LEAD_TIME_THRESHOLD \
                    and self.in_interval(x,y):
                    return sum([wi * qi for wi,qi in zip(self.weights, \
                        mt.calc_quality(x,y))])
                else:
                    return 0.0
            self.sim_fn = sim_fn

        if self.cost_matrix_fn is None:
            self.cost_matrix_fn = lambda cost: self.max_similarity_score - cost

        if self.yield_condt is None:
            self.yield_condt = lambda sim_score : sim_score > 0.0

    def in_interval(self, warning, gsr_event):
        return mt.calc_event_date_delta_days(warning,gsr_event) <= \
            self.scoring_interval

    def do_matching(self, warnings, gsr_events, start, end):
        item_key = lambda e : iso_str_dt(e['eventDate'])
        fwarnings   = filter_by_date(start, end, warnings, item_key)
        fgsr_events = filter_by_date(start, end, gsr_events, item_key)
        matches = []
        per_co = lambda evts : group_by(evts, key=lambda x:x['location'][0])
        fwarnings, fgsr_events = per_co(fwarnings), per_co(fgsr_events)
        for match in do_hungarian_assignment(fwarnings, fgsr_events, \
            self.sim_fn,self.yield_condt, cost_matrix_func=self.cost_matrix_fn):
            matches.append(match)
        return fwarnings, fgsr_events, matches

