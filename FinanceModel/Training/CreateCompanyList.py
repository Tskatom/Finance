import json
from Util import common
import os

def get_company_list():
    comDir = common.get_configuration("model", "COMPANY_MEMBER")
    sfile = os.listdir(comDir)
    companyList = {}
    for fi in sfile:
        with open(comDir+"/"+fi) as comFile:
            lines = comFile.readlines()
            stockIndex = lines[1].replace("\r","").replace("\n","").split(",")[1].replace(" Index","")
            if stockIndex not in companyList:
                companyList[stockIndex] = []
            for line in lines[2:]:
                infos = line.replace("\r","").replace("\n","").split(",")
                companyName = infos[2]
                tmps = companyName.split(" ")
                companyName = " ".join(tmps[:len(tmps)-1 if len(tmps)>1 else len(tmps)])
                if companyName not in companyList[stockIndex]:
                    companyList[stockIndex].append(companyName)
            companyList[stockIndex].append(stockIndex)
    
    desFile = common.get_configuration("model", "COMPANY_LIST")
    with open(desFile,"w") as output:
        jsStr = json.dumps(companyList)
        output.write(jsStr)


def test():
    get_company_list()
    
if __name__ == "__main__":
    test()
    