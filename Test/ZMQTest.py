#-*-coding:utf8-*-
from etool import queue,logs
import json
import os
import sys

def test():
    queue.init()
    port = 'tcp://*:30115'
    with queue.open(port,'w',capture=True) as outq:
        msgObj = {'embersId': 'f0c030a20e28a12134d9ad0e98fd0861fae7438b', 'confidence': 0.13429584033181682, 'strength': '4', 'derivedFrom': [u'5df18f77723885a12fa6943421c819c90c6a2a02', u'be031c4dcf3eb9bba2d86870683897dfc4ec4051', u'3c6571a4d89b17ed01f1345c80cf2802a8a02b7b'], 'shiftDate': '2011-08-08', 'shiftType': 'Trend', 'location': u'Colombia', 'date': '2012-10-03', 'model': 'Finance Stock Model', 'valueSpectrum': 'changePercent', 'confidenceIsProbability': True, 'population': 'COLCAP'}
        outq.write(msgObj)
    
    print "Success"
    pathName = os.path.dirname(sys.argv[0])
    print pathName

def testFile():
    filePath = "./stock_2012-10-01.txt"
    with open(filePath,'r') as readFile:
        lines = readFile.readlines()
        for line in lines:
            print line.replace("\n","").replace("\r","")

def testLog():
    __processor__ = 'TestLog'
    log = logs.getLogger(__processor__)
    logs.init()
    log.info("Error: %s" % "I'm Here")
                
test()
    