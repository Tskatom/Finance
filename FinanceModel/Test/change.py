import json
from Util import common
negativeFilePath = common.get_configuration("training", "NEGATIVE_DIC")
negativeDoc  = open(negativeFilePath).readlines()
neg = []
for line in negativeDoc:
    neg.append(line.strip())
    
positiveFilePath = common.get_configuration("training", "POSITIVE_DIC")
positiveDoc  = open(positiveFilePath).readlines()
pog = []
for line in positiveDoc:
    pog.append(line.strip())

with open("c:/negative.json","w") as out_q:
    out_q.write(json.dumps(neg))
    
with open("c:/positive.json","w") as out_q:
    out_q.write(json.dumps(pog))