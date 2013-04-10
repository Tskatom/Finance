#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = "Wei Wang"
__email__ = "tskatom@vt.edu"


import hashlib
from datetime import datetime
from etool import queue


class warning():
    def __init__(self, model_name):
        self.warning = {}
        self.probability_flag = "confidenceIsProbability"
        self.warning[self.probability_flag] = "True"
        self.date_lab = "date"
        self.event_date_lab = "eventDate"
        self.population_lab = "population"
        self.probability_lab = "confidence"
        self.model_lab = "model"
        self.warning[self.model_lab] = model_name
        self.id_lab = "embersId"
        self.derived_lab = "derivedFrom"
        self.event_type_lab = "eventType"
        self.comment_lab = "comments"
        self.location_lab = "location"

    def send(self, pub_zmq):
        with queue.open(pub_zmq, "w") as q_w:
            q_w.write(self.warning)

    def generateIdDate(self):
        if not self.id_lab in self.warning:
            self.warning[self.id_lab] = hashlib.sha1(str(self.warning)).hexdigest()
        if not self.date_lab in self.warning:
            self.warning[self.date_lab] = datetime.utcnow().isoformat()

    def setEventDate(self, event_date):
        self.warning[self.event_date_lab] = event_date

    def setPopulation(self, population):
        self.warning[self.population_lab] = population

    def setProbability(self, probability):
        self.warning[self.probability_lab] = probability

    def setDerivedFrom(self, derived):
        self.warning[self.derived_lab] = derived

    def setEventType(self, event_type):
        self.warning[self.event_type_lab] = event_type

    def setComments(self, comment):
        self.warning[self.comment_lab] = comment

    def setDate(self, deliver_date):
        self.warning[self.date_lab] = deliver_date

    def setLocation(self, location):
        self.warning[self.location_lab] = location
