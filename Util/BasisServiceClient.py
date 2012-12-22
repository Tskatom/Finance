# -*- coding: utf-8 -*-
import zmq


class BasisEnricher(object):
    def __init__(self, socketId):
        self.context = zmq.Context(1)
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect(socketId)

    def requestJson(self, j):
        self.socket.send_json(j, 0)
        j = self.socket.recv_json(0)
        return j

    def requestStr(self, jsonStr):
        if not isinstance(jsonStr, unicode):
            jsonStr = jsonStr.decode('utf-8')
        self.socket.send_unicode(jsonStr)
        return self.recv_unicode(jsonStr)

if __name__ == "__main__":
    bS = BasisEnricher('tcp://54.234.11.20:5563')
    print bS.requestJson({'text': 'this is a test message.'})
