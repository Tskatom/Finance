#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# vim: ts=4 sts=4 sw=4 tw=79 sta et
"""%prog 
Run historical twitter feeds for Civil Unrest modeling using
spatial scan.
"""

__author__ = 'Rupen'
__email__ = 'rupen.paul@cs.vt.edu'
import xlwt
from utils import iso_str_dt, group_by, \
                  calc_months_from_time_range
from evaluation.metrics import CUMetrics as mt
from datetime import datetime
import argparse

MONTH_YEAR = "%b %Y"

def arg_parse():
    ap = argparse.ArgumentParser("The automatic training and test tools")
    ap.add_argument('-s',dest='start',type=str,help='The start time of evaluation')
    ap.add_argument('-e',dest='end',type=str,help='The end time of evaluation')
    ap.add_argument('-ev',dest="event_type",type=str,default="041",nargs="?",help='The type of event')
    ap.add_argument('-gsr',dest="gsr_file",type=str,default="./data/all_gsr_warnings.txt",nargs="?",help='The path of gsr event')
    ap.add_argument('-wr',dest="warning_file",type=str,default="./data/warning.txt",nargs="?",help='The path of warning')
    
    return ap.parse_args()

def do_evaluation_global_optimum(warnings, gsr_events, start, end):
    from evaluation.matching import MatchingGlobalOptimum as MGO
    fwarnings,fgsr_events, matches = MGO().do_matching(warnings,gsr_events, \
                                                       start, end)
    
    #osi evaluation is based on per month evaluation
    matches = group_by(matches,key=lambda m: m[0]['location'][0])
    print matches
    "get the list of fetched warning"
    matched_warning = []
    
    months = calc_months_from_time_range(start,end)
    countries = set(fwarnings.keys()) | set(fgsr_events.keys())
    get_year_month = lambda e : iso_str_dt(e['eventDate']).strftime(MONTH_YEAR)
    for co in fwarnings:
        fwarnings[co] = group_by(fwarnings[co],key=get_year_month)
    for co in fgsr_events:
        fgsr_events[co] = group_by(fgsr_events[co],key=get_year_month)
    for co in matches:
        for m in matches[co]:
            matched_warning.append(m[0]['embersId'])
        matches[co] = group_by(matches[co],key=lambda x: get_year_month(x[1]))
    print matched_warning
    
    
    for m in months:
        mo = m.strftime(MONTH_YEAR)
        #we add another step for country wise evaluation
        for co in countries:
            gsr_events_mo_co, warnings_mo_co, matches_mo_co,warnings_mo_co_list = [[]] * 4
            if co in fgsr_events and mo in fgsr_events[co]:
                gsr_events_mo_co =  fgsr_events[co][mo]
            if co in fwarnings and mo in fwarnings[co]:
                warnings_mo_co =  [ w['embersId'] for w in fwarnings[co][mo] ]
                warnings_mo_co_list = [w for w in fwarnings[co][mo]]
            if co in matches and mo in matches[co]:
                matches_mo_co =  matches[co][mo]
            tot_gsr_mo_co = len(gsr_events_mo_co)
            matched_warnings_mo_co =  [ ma[0]['embersId'] \
                                        for ma in matches_mo_co ]
            tot_warnings_mo_co = len(set(warnings_mo_co) | \
                                     set(matched_warnings_mo_co))
            tot_matches_mo_co = len(matches_mo_co)
            yield co, mo, tot_gsr_mo_co, tot_warnings_mo_co, \
                  tot_matches_mo_co, \
                  (mt.calc_precision_recall(tot_matches_mo_co, \
                                            tot_warnings_mo_co, \
                                            tot_gsr_mo_co)), \
                  (mt.calc_mean_scores(matches_mo_co,matched_warning,warnings_mo_co_list))

def main():
    from utils import load_gsr_warnings,load_warnings
    args = arg_parse()
    start = args.start
    end = args.end
    event_type = args.event_type
    gsr_file = args.gsr_file
    warning_file = args.warning_file
    
    cu_gsr_events = load_gsr_warnings(filename=gsr_file,eventType=event_type)
    warnings = load_warnings(filename=warning_file)
    t_format = "%Y-%m-%d"
    start = datetime.strptime(start,t_format)
    end = datetime.strptime(end,t_format)
    wbk = xlwt.Workbook()
    sheet = wbk.add_sheet('report')
    sheet.write(0,0,'Country')
    sheet.write(0,1,'Year-Month')
    sheet.write(0,2,'GSR')
    sheet.write(0,3,'Prediction')
    sheet.write(0,4,'Matched')
    sheet.write(0,5,'Precision')
    sheet.write(0,6,'Recall')
    sheet.write(0,7,'Mean-Quality')
    sheet.write(0,8,'Mean-Lead-Time')
    sheet.write(0,9,'Mean-Probalility')
    i = 1
    summary = {}
    for e in do_evaluation_global_optimum(warnings, cu_gsr_events, start, end):
        if e[0] not in summary:
            summary[e[0]] = {}
            summary[e[0]]["gsr"] = e[2]
            summary[e[0]]["prediction"] = e[3]
            summary[e[0]]["matched"] = e[4]
        else:
            summary[e[0]]["gsr"] += e[2]
            summary[e[0]]["prediction"] += e[3]
            summary[e[0]]["matched"] += e[4]
            
        sheet.write(i,0,e[0])
        sheet.write(i,1,e[1])
        sheet.write(i,2,e[2])
        sheet.write(i,3,e[3])
        sheet.write(i,4,e[4])
        sheet.write(i,5,e[5][0])
        sheet.write(i,6,e[5][1])
        sheet.write(i,7,e[6][0])
        sheet.write(i,8,e[6][1])
        sheet.write(i,9,e[6][2])
        i += 1
    wbk.save('report.xls')

    for k,v in summary.items():
        if v["prediction"] == 0:
            if v["gsr"] == 0:
                v["precision"] = 1.0
                v["recall"] = 1.0
            else:
                v["precision"] = .0
                v["recall"] = .0
        else:        
            v["precision"] = 1.0*v["matched"]/v["prediction"]
            v["recall"] = 1.0*v["matched"]/v["gsr"]
        print k," : ", v
if __name__ == "__main__":
    main()
