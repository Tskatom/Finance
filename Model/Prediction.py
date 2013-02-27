#-*- coding:utf8-*-

import WarningCreate
import RawNewsProcess
import RawStockProcess
import argparse

def execute(predictionDate,rawStockFilePath,rawNewsFilePath,cfgFilePath):
    #process raw Stock Process data
    RawStockProcess.execute(rawStockFilePath,cfgFilePath)
    #process raw news data
    RawNewsProcess.execute(rawNewsFilePath,cfgFilePath)
    #Warning Create
    WarningCreate.execute(predictionDate,cfgFilePath)
    
def parse_args():
    ap = argparse.ArgumentParser("Process the input and then output the prediction result")
    ap.add_argument('-c','--conf',metavar="CONFIG",type=str,default='../Config/config.cfg',nargs='?',help='the config file path')
    ap.add_argument('-s','--stock',metavar="STOCK INDEX", type=str,nargs='?',help='the path of daily stock value' )
    ap.add_argument('-n','--news',metavar="NEWS", type=str,nargs='?',help='the path of daily news')
    ap.add_argument('-d','--day',metavar="PREDICTIVEDAY", type=str,nargs='?',help='The day to be predicted' )
    return ap.parse_args() 

def main():
    # get the initiate parameters
    args = parse_args()
    configFile = args.conf
    predictionDay = args.day
    stockFile = args.stock
    newsFile = args.news
    
    # execute the prediction
    execute(predictionDay,stockFile,newsFile,configFile)
         
if __name__ == "__main__":
    main()