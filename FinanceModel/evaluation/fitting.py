#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# vim: ts=4 sts=4 sw=4 tw=79 sta et
"""%prog 

"""

__author__ = 'Rupen'
__email__ = 'rupen@cs.vt.edu'

from run_historics import *
from evaluations_iarpa import *
import itertools

params = ['location_thresh','keyword_thresh',
          'angle_thresh','extrapol_ratio','look_back_days',
          'pred_future_days','min_chain_len','duplicate_period_days'
         ] 

files = { "3" : { "files": ["20120620_20120724_3H_90M","20120724_20120824_3H_90M",
                            "20120824_20120924_3H_90M"], 
                  "win_size":180,
                  "win_step":90
                },
          "12" : { "files": ["20120620_20120924_12H_6H"], 
                   "win_size":720,"win_step":360
                 },
          "6": { "files": ["20120620_20120724_6H_3H","20120724_20120824_6H_3H",
                           "20120824_20120924_6H_3H"], 
                 "win_size":360,"win_step":180
               }
        }


def aggregate_eval_stats(filename):
    lines = open(filename,'r').readlines()
    co_set = {}
    for l in lines[1:]:
        a = l.split("\t")
        if a[1] not in co_set:
            co_set[a[1]] = {}
        if a[0] not in co_set[a[1]]:
           co_set[a[1]][a[0]] = [0.0] * 5
        co_set[a[1]][a[0]][0] += float(a[3])
        co_set[a[1]][a[0]][1] += float(a[4])
        co_set[a[1]][a[0]][2] += float(a[5])
        co_set[a[1]][a[0]][3] += float(a[8])
        co_set[a[1]][a[0]][4] += float(a[9])
    from evaluation.metrics import CUMetrics as mt
    out = open("aresults12.tsv",'w')
    header = "country\tsetting\ttot_gsr\ttot_preds\ttot_matches\t"
    header += "prec\trecall\tmean_qual\tmean_lead\n"
    out.write(header)
    for co in co_set:
        for se in co_set[co]:
            p,r = mt.calc_precision_recall(co_set[co][se][2],\
                                           co_set[co][se][1],\
                                           co_set[co][se][0])
            out.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n"% (co,se,co_set[co][se][0],\
                      co_set[co][se][1],co_set[co][se][2],p,r, co_set[co][se][3]/4.0, \
                      co_set[co][se][4]/4.0 ))
    out.close()

def param_fitting(winSize):
    loc_thresh = [0.6,0.7]
    key_thresh = [0.6,0.7]
    angle_thresh = [7.0,10.0]
    extrapol_ratio = [0.75,0.90,0.95]
    look_back_days = [7]
    pred_future_days = [7,14]
    min_chain_len = [2,3,4,5]
    duplicate_period_days = [0,1]
    return [p for p in itertools.product(loc_thresh,key_thresh,
                                         angle_thresh,extrapol_ratio,look_back_days,
                                         pred_future_days,min_chain_len,duplicate_period_days
                                        )
           ]

if __name__ == '__main__':
    import sys
    winSize = sys.argv[1].strip()
    settings = param_fitting(winSize)
    print "Total Settings %s" % (len(settings))

    settings_out = open("settings" + winSize + ".tsv",'w')
    #results_out1 = open("results1" + winSize + ".tsv",'w')
    results_out2 = open("results2" + winSize + ".tsv",'w')

    header0 = "win_size\twin_step\tloc_thresh\tkey_thresh\tangle_thresh\textrapol_ratio\t"
    header0 += "look_back_days\tpred_future_days\tmin_chain_len\tduplicate_period_days\n"
    settings_out.write(header0)
    '''
    header1 = "setting\ttot_gsr\ttot_predications\ttot_matches\tmean_prec\tmean_recall\t"
    header1 += "mean_qual\tmean_lead\tmean_prob\n"
    results_out1.write(header1)
    '''
    header2 = "setting\tperiod\tcountry\ttot_gsr\ttot_predications\ttot_matches\t"
    header2 += "mean_prec\tmean_recall\t"
    header2 += "mean_qual\tmean_lead\tmean_prob\n"
    results_out2.write(header2)

    from utils import load_gsr_warnings, gsrevent_str_dt 
    cu_gsr_events = load_gsr_warnings(eventType='01')
    counter = 0
    fargs = {}
    fargs["win_files"] = [ "../../data_historic/" + f for f in files[winSize]['files'] ]
    clusters = read_all_clusters(fargs)
    for setting in settings:
        counter += 1
        print "Setting #%s" % (counter)

        args = {"win_files": fargs["win_files"]}
        args["start_date"] = gsrevent_str_dt("2012-06-24")
        args["end_date"] = gsrevent_str_dt("2012-09-30")
        args["win_size"] = files[winSize]['win_size']
        args["win_step"] = files[winSize]['win_step']
        for a,val in zip(params,setting):
            args[a] = val

        build_args(args)
        pred_warnings = get_predicted_warnings(clusters,args)
        
        settings_out.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % \
             (args['win_size'],args['win_step'],args["location_thresh"],args["keyword_thresh"],
              args["angle_thresh"],args["extrapol_ratio"],args["look_back_days"],
              args["pred_future_days"],args["min_chain_len"],args["duplicate_period_days"]) )

        for e in  do_evaluation_global_optimum(pred_warnings, \
                                               cu_gsr_events, \
                                               args['start_date'], \
                                               args['end_date']):
            results_out2.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
                        % (counter,e[0],e[1],e[2],e[3],e[4], \
                           e[5][0],e[5][1],e[6][0],e[6][1],e[6][2]))
            results_out2.flush()

    settings_out.close()
    #results_out1.close()
    results_out2.close()
