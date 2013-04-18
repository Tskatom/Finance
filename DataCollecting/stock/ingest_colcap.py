#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = "Wei Wang"
__email__ = "tskatom@vt.edu"

"""

https://www.grupoaval.com/portales/jsp/historicotabla.jsp?indi=4795&fecini=04/17/2013&fecfin=04/18/2013
http://www.bolchile.cl/portlets/CentroDatosPortlet/RecuperarDatosServlet?idioma=en&tipoInstr=INDICES_MERCADO&instrumento=CHILE65&vista=PRECIOS&fechaInicio=15/03/2013&fechaTermino=18/04/2013&temporalidad=DIA&fechaComposicion=18/04/2013

"""

from etool import message, logs, args, queue
from bs4 import BeautifulSoup
import urllib2
import json


def ingest_url(url):
    try:
        soup = BeautifulSoup(urllib2.urlopen(url))
        #get the latest two day's data

        datas = soup.find_all('tr')
        if len(datas) == 0:
            return None

if __name__ == "__main__":
    pass

