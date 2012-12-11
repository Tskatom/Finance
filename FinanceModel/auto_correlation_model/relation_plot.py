from __future__ import print_function
import numpy
from matplotlib.mlab import csv2rec
from pylab import figure, show
import matplotlib.cbook as cbook
from matplotlib.ticker import Formatter
import json
from datetime import datetime

datafile = cbook.get_sample_data('msft.csv', asfileobj=False)
print ('loading %s' % datafile)
r = csv2rec(datafile)[-40:]
print (type(r))

class MyFormatter(Formatter):
    def __init__(self, dates, fmt='%Y-%m-%d'):
        self.dates = dates
        self.fmt = fmt

    def __call__(self, x, pos=0):
        'Return the label for time x at position pos'
        ind = int(round(x))
        if ind>=len(self.dates) or ind<0: return ''

        return self.dates[ind].strftime(self.fmt)

formatter = MyFormatter(r.date)

t_list = json.load(open('d:/embers/relation_results.json','r'))
t_list_indu = t_list["INDU"]
p_dates = []
zscore30s = []
names = []
counts = []

for v in t_list_indu:
    p_dates.append(datetime.strptime(v['post_date'],"%Y-%m-%d"))
    zscore30s.append(round(v['zscore30'],4))
    names.append(v['p_stock'])
    counts.append(len(v['related_events']))

r_indu =  numpy.core.records.fromarrays([p_dates,zscore30s,names,counts], names=('p_date,zscore30,name,count'))

formatter2 = MyFormatter(r_indu.p_date)
fig2 = figure()
ax = fig2.add_subplot(111)
ax.xaxis.set_major_formatter(formatter2)
ax.plot(numpy.arange(len(numpy.arange(len(r_indu)))), r_indu.count, 'o-')
fig2.autofmt_xdate()


show()

