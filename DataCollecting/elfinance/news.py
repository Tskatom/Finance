#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = "Wei Wang"
__email__ = "tskatom@vt.edu"


class News():
    def __init__(self):
        self.news = {}
        self.title_lab = "title"
        self.post_time_lab = "postTime"
        self.author_lab = "author"
        self.url_lab = "url"
        self.date_lab = "date"
        self.source_lab = "source"
        self.content_lab = "content"
        self.encoding_lab = "encoding"
        self.id_lab = "embersId"
        self.country_lab = "countries"
        self.news[self.country_lab] = []
        self.labels_lab = "labels"
        self.news[self.labels_lab] = []

    def set_title(self, title):
        self.news[self.title_lab] = title

    def set_posttime(self, post_time):
        self.news[self.post_time_lab] = post_time

    def set_author(self, author):
        self.news[self.author_lab] = author

    def set_url(self, url):
        self.news[self.url_lab] = url

    def set_date(self, date):
        self.news[self.date_lab] = date

    def set_source(self, source):
        self.news[self.source_lab] = source

    def set_content(self, content):
        self.news[self.content_lab] = content

    def set_encoding(self, encoding):
        self.news[self.encoding_lab] = encoding

    def set_id(self, embersId):
        self.news[self.id_lab] = embersId

    def set_country(self, country):
        self.news[self.country_lab].append(country)

    def set_labels(self, label):
        self.news[self.labels_lab].append(label)