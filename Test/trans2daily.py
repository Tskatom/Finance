import json
import datetime
import hashlib
from Util import common
"""
postTime, postDate,author,url,company,title,updateTime,content,embersId,stockIndex,date,updateDate,source:Bloomberg News
2012-11-08T19:05:30.451366
"""
def q_time_format(queryTime):
    ft = "%Y-%m-%d %H:%M:%S"
    n_q_t = datetime.datetime.strptime(queryTime,ft)
    return n_q_t.isoformat()

def u_date_format(updateTime):
    ft = "%Y-%m-%d %H:%M:%S.0"
    n_d_t = datetime.datetime.strptime(updateTime,ft)
    return n_d_t.isoformat()

i = 0
bl_news = json.load(open('d:/embers/FinanceModel/output/BBNews-Group-Stock.json','r'))
with open("d:/embers/FinanceModel/output/daily_archieved_news.json","w") as out_q:
    for (k,v) in bl_news.items():
        for (d_k,d_v) in v.items():
            author = d_v["author"]
            postTime = u_date_format(d_v["postTime"])
            relatedCompany = ""
            queryTime = q_time_format(d_v["queryTime"])
            content = d_v["content"]
            source = "Bloomberg News"
            url = d_v["newsUrl"]
            title = d_v["title"]
            stockIndex = k
            new_format = {}
            
            new_format["postTime"] = postTime
            new_format["postDate"] = postTime.split("T")[0]
            new_format["author"] = author
            new_format["url"] = url
            new_format["company"] = ""
            new_format["title"] = title
            new_format["content"] = content
            new_format["stockIndex"] = stockIndex
            new_format["date"] = queryTime
            new_format["updateDate"] = queryTime.split("T")[0]
            new_format["source"] = source
            
            embersId = hashlib.sha1(str(new_format)).hexdigest()
            new_format["embersId"] = embersId
            
            out_q.write(str(new_format))
            out_q.write("\n")
            i = i + 1
print i

negativeFilePath = common.get_configuration("training", "NEGATIVE_DIC")
negativeDoc  = open(negativeFilePath).readlines()
negativeWords = []
nw_words_str = ""
for l in negativeDoc:
    negativeWords.append(l.replace("\n",""))
    nw_words_str = nw_words_str + l.replace("\n","") + " "
print nw_words_str   

"Read the Positive Finance Dictionary"
positiveFilePath = common.get_configuration("training", "POSITIVE_DIC")
positiveDoc = open(positiveFilePath).readlines()
postiveWords = []
po_words_str = ""
for line in positiveDoc:
    postiveWords.append(line.replace("\n",""))
    po_words_str = po_words_str + line.replace("\n","") + " "

print po_words_str  
