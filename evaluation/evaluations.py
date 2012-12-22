#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# vim: ts=4 sts=4 sw=4 tw=79 sta et
"""%prog 
Run historical twitter feeds for Civil Unrest modeling using
spatial scan.
"""

__author__ = 'Rupen'
__email__ = 'rupen.paul@cs.vt.edu'

from datetime import datetime
from time import strptime
import json
from utils import str_dt, TIME_ISOFORMAT, TIME_GSRFORMAT
from utils import group_by, do_hungarian_assignment

def get_precision_recall(matched_relevant_predicted_warnings, 
                         tot_predicted_warnings, tot_gsr_warnings):
    if len(tot_predicted_warnings) == 0:
        prec = 0.0
    else:
        prec = len(matched_relevant_predicted_warnings) / float(len(tot_predicted_warnings))
    recall = len(matched_relevant_predicted_warnings) / float(len(tot_gsr_warnings))
    return (round(prec * 100) / 100.0), (round(recall * 100) / 100.0)

#TODO include pop. type and warning eventType 
def get_quality_score(predicted_warning,gsr_warning,pred_future_days=7):

    days = abs(predicted_warning['eventDate'] - gsr_warning['eventDate']).days
    date_score = 0.0 
    if days <= pred_future_days:
        date_score = 1.0 - (min(days,pred_future_days) / float(pred_future_days))
    if date_score < 0.0:
        date_score = 0.0
    co,st,ci = 0.0,0.0,0.0
    if  predicted_warning['location'][0] == gsr_warning['location'][0]:
        co = 1.0
    if ( gsr_warning['location'][1] == '-' and gsr_warning['location'][2] == '-' ) or predicted_warning['location'][1] == gsr_warning['location'][1]:
        st = 1.0
    if gsr_warning['location'][2] == '-' or \
        predicted_warning['location'][2] == gsr_warning['location'][2]:
        ci = 1.0

    loc_agg_score = (co + co * st + co * st * ci)/3.0
    #return { "date_score": date_score * co * st, "loc_score": loc_agg_score }
    return [date_score * co * st, loc_agg_score]

def get_lead_time_score(predicted_warning,gsr_warning):
    if gsr_warning['date'] <= predicted_warning['date']:
        #it would be better if gsr timestamp included hr:min atleast
        return 0.0
    else:
        diff_hrs = (gsr_warning['date'] - predicted_warning['date']).seconds / 3600.0
        if diff_hrs < 12.0:
            return 0.0
        else:
            return round(diff_hrs/24.0)

def get_prob_score(predicted_warning):
    o = 1#ground truth probability
    diff = o - predicted_warning['confidence']
    return 1 - (diff * diff)

def do_matching(predicted_warnings,gsr_warnings,pred_future_days=7):
    max_quality_score = 2
    max_lead_score = pred_future_days
    max_prob_score = 1
    max_score = max_quality_score + max_lead_score + max_prob_score

    #cost_fn = lambda x,y : get_quality_score(x,y,pred_future_days)
    def cost_fn(x,y):
        q = get_quality_score(x,y,pred_future_days)
        l = get_lead_time_score(x,y)
        p = get_prob_score(x)
        if l is None:
            print "lead is none"
            l = 0.0
        if p is None:
            print "prob is none"
            p = 0.0
        return q[0] + q[1] + l + p
        
    #cost_matrix_func = lambda cost: max_score - (cost['date_score'] + cost['loc_score'])
    cost_matrix_func = lambda cost: max_score - cost
    #yield_condt = lambda scr : (scr['date_score'] + scr['loc_score']) > 0.0 
    #yield_condt = lambda scr : scr > 0.0 
    yield_condt = lambda scr : scr > 1.0 
    warnings_by_co = lambda warnings : group_by(warnings,key=lambda x: x['location'][0])
    return do_hungarian_assignment(warnings_by_co(predicted_warnings),
                                         warnings_by_co(gsr_warnings),
                                         cost_fn,yield_condt,cost_matrix_func=cost_matrix_func)


def get_avg_scores(matches):
    count = 0
    avgQualScore = 0
    avgLeadScore = 0
    avgProbScore = 0
    for m in matches:
        avgQualScore += m[2][0] + m[2][1]
        avgLeadScore += m[3]
        avgProbScore += m[4]
        count += 1

    if count == 0:
        return 0.0,0.0,0.0

    avgQualScore = round((avgQualScore/float(count)) * 100.0) / 100.0
    avgLeadScore = round((avgLeadScore/float(count)) * 100.0) / 100.0
    avgProbScore = round((avgProbScore/float(count)) * 100.0) / 100.0

    return avgQualScore, avgLeadScore, avgProbScore

def filter_warnings(args,warnings,time_format=TIME_ISOFORMAT):
    start_date = datetime(*strptime(args['start_date'],TIME_GSRFORMAT)[0:6])
    end_date = datetime(*strptime(args['end_date'],TIME_GSRFORMAT)[0:6])
    filtered_warnings = [] 
    for warning in warnings:
        warning['date'] = datetime(*strptime(warning['date'],time_format)[0:6])
        warning['eventDate'] = datetime(*strptime(warning['eventDate'],time_format)[0:6])
        if warning['eventDate'] <= end_date and warning['eventDate'] >= start_date:
            filtered_warnings.append(warning)
    return filtered_warnings

def load_cu_gsr_warnings(args):
    warnings = [ json.loads(w) for w in open(args['gsr_warnings_file'],'r').readlines()]
    #warnings grouped by eventId
    warnings = group_by(warnings,key=lambda w : w['eventId'][:2])
    #Civil UNrest (01) warnings
    warnings = warnings['01']
    for w in warnings:
        w['location'] = [ l.strip() for l in w['location'].split(',')]
        while len(w['location']) < 3:
            w['location'].append('-')
    return warnings

def print_evaluation(predicted_warnings,gsr_warnings,tot_matches,relevant_matches):
    precision,recall = get_precision_recall(relevant_matches,predicted_warnings,gsr_warnings)
    tot_avg_qual_score, tot_avg_lead_score, tot_avg_prob_score = get_avg_scores(tot_matches)
    relv_avg_qual_score, relv_avg_lead_score, relv_avg_prob_score = get_avg_scores(relevant_matches)
    print "Relevant Matches= %s Total Matches= %s" % (len(relevant_matches),len(tot_matches))
    print "Total Predictions= %s Total GSR= %s" % ( len(predicted_warnings), len(gsr_warnings))
    print "Precision= %s Recall= %s" % (precision,recall)
    print "Total Avg Score Quality= %s Lead= %s Prob = %s" % (tot_avg_qual_score,tot_avg_lead_score,
                                                              tot_avg_prob_score)
    print "Relevant Avg Score Quality= %s Lead= %s Prob = %s" % (relv_avg_qual_score,
                                                                 relv_avg_lead_score,
                                                                 relv_avg_prob_score)

def do_evaluation(predicted_warnings,args):
    filtered_pred_warnings = filter_warnings(args,predicted_warnings)
    filtered_gsr_warnings = filter_warnings(args,load_cu_gsr_warnings(args),
                                            time_format=TIME_GSRFORMAT)

    tot_matches = []
    relevant_matches = []
    for match in do_matching(filtered_pred_warnings,filtered_gsr_warnings,args['pred_future_days']):
        pred_warning = match[0]
        gsr_warning = match[1]
        quality_score = get_quality_score(pred_warning,gsr_warning,args['pred_future_days'])
        lead_time_score = get_lead_time_score(pred_warning,gsr_warning)
        prob_score = get_prob_score(pred_warning)
        if quality_score[0] > args['date_score_thresh'] and \
           quality_score[1] > args['loc_score_thresh']   and \
           lead_time_score > args['lead_time_thresh']:
            relevant_matches.append((pred_warning,gsr_warning,
                                     quality_score,lead_time_score,prob_score))
        tot_matches.append((pred_warning,gsr_warning,
                            quality_score,lead_time_score,prob_score))
    #print_evaluation(filtered_pred_warnings,filtered_gsr_warnings,tot_matches,relevant_matches)
    return filtered_pred_warnings, filtered_gsr_warnings, tot_matches, relevant_matches

def build_eval_args(args):
    if args is None:
        args = { }
    if 'gsr_warnings_file' not in args:
        args['gsr_warnings_file'] = 'data/all_gsr_warnings.txt'
    if 'start_date' not in args:
        args['start_date'] = '2012-06-24' 
    if 'end_date' not in args:
        args['end_date'] = '2012-10-01'
    if 'pred_future_days' not in args:
        args['pred_future_days'] = 7
    if 'date_score_thresh' not in args:
        args['date_score_thresh'] = 0.4 
    if 'loc_score_thresh' not in args:
        args['loc_score_thresh'] = 0.34
    if 'lead_time_thresh' not in args:
        args['lead_time_thresh'] = -1

if __name__ == "__main__":
    pass
