#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = "Wei Wang"
__email__ = "tskatom@vt.edu"

import sys
import os
import xlrd
import argparse
import sqlite3 as lite
import datetime
import re


DATE_FORMAT = "%Y-%m-%d"

def get_date(date_value, datemode):
    t_tuple = xlrd.xldate_as_tuple(date_value,datemode)
    return datetime.date(t_tuple[0],t_tuple[1],t_tuple[2])

def get_time_str(date):
    return datetime.datetime.strftime(date, DATE_FORMAT)


class GSR_Event:
    def __init__(self, gsr_file, db_conn):
        self.gsr_file = gsr_file
        self.gsr_events = list()
        self.db_conn = db_conn

    def load_gsr(self):
        gsr_book = xlrd.open_workbook(self.gsr_file)
        gsr_sheet = gsr_book.sheet_by_index(0)
        g_len = gsr_sheet.nrows
        for i in range(1, g_len):
            row = gsr_sheet.row_values(i)
            event_id = row[0]
            event_sub_id = row[1]
            country = row[4]
            state = row[5]
            city = row[6]
            event_code = row[7]
            population = row[8]
            try:
                event_date = get_time_str(get_date(row[9],gsr_book.datemode))
                earliest_date = get_time_str(get_date(row[10],gsr_book.datemode))
            except Exception as e:
                print "An Error occur when Extracting Date field: ", e.args[0]
                continue
            
            source = row[11]
            if re.search('^04',event_code):
                event = {"eventId":event_id, "eventSubId":event_sub_id, "country":country, \
                        "state":state, "city":city, "eventCode":event_code, "population":population, \
                        "eventDate":event_date, "earliestDate":earliest_date, "source":source}
                self.gsr_events.append(event)

    def clear_db(self):
        sql = "delete from gsr_event"
        c = self.db_conn.cursor()
        c.execute(sql)
        self.db_conn.commit()

    def commit_db(self):
        clear_db()

        sql = "insert into gsr_event (event_id, event_sub_id,country,state,city,event_code,population,\
                event_date,earliest_date,source) values (?,?,?,?,?,?,?,?,?,?)"
        c = self.db_conn.cursor()

        for event in self.gsr_events:
            event_id = event["eventId"]
            event_sub_id = event["eventSubId"]
            country = event["country"]
            state = event["state"]
            city = event["city"]
            event_code = event["eventCode"]
            population = event["population"]
            event_date = event["eventDate"]
            earliest_date = event["earliestDate"]
            source = event["source"]
            try:
                c.execute(sql,[event_id, event_sub_id, country, state, city, event_code, population, event_date,earliest_date, source])
            except lite.Error as e:
                print "An Error occur: ", e.args[0]
        c.close()
        self.db_conn.commit()



def parse_args():
    ap = argparse.ArgumentParser("Import the gsr data")
    ap.add_argument('-g', dest="gsr_file", type=str, help="The paht of gsr file")
    ap.add_argument('-d', dest='db_file', type=str, help='The path of dbfile')
    return ap.parse_args()

def main():
    args = parse_args()
    gsr_file = args.gsr_file
    db_file = args.db_file
    db_conn = lite.connect(db_file)
    gsr = GSR_Event(gsr_file, db_conn)
    gsr.load_gsr()
    gsr.commit_db()

if  __name__ == "__main__":
    main()

