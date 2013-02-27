import boto
import json

global TREND_RANGE
def get_trendtype(name,changePercent):
    tJson = TREND_RANGE[name]
    
    distance = 10000
    trendType = None
    for changeType in tJson:
        tmpDistance = min(abs(changePercent-tJson[changeType][0]),abs(changePercent-tJson[changeType][1]))
        if tmpDistance < distance:
            distance = tmpDistance
            trendType = changeType
    
    return trendType

def update_records(conn,stock_list):
    t_domain = conn.get_domain("t_enriched_bloomberg_prices")
    i = 0
    for stock in stock_list:
        sql = "select embersId,changePercent,trendType from t_enriched_bloomberg_prices where name = '{}' and postDate >='2003-01-01' and postDate < '2012-12-17' order by postDate asc ".format(stock)
        rs = t_domain.select(sql)
        for r in rs:
            embersId = r["embersId"]
            changePercent = float(r["changePercent"])
            o_trend = r["trendType"]
            
            trendType = get_trendtype(stock,changePercent)
            att = t_domain.get_attributes(embersId)
            att["trendType"] = trendType
            att.save()
            i += 1
            print i
            
def main():
    global TREND_RANGE
    trend_file = "./trendRange.json"
    
    trendObject = None
    with open(trend_file,"r") as tFile:
        trendObject = json.load(tFile)
    "Get the latest version of Trend Ranage"
    trend_versionNum = max([int(v) for v in trendObject.keys()])
    "To avoid changing the initiate values, we first transfer the json obj to string ,then load it to create a news object"
    TREND_RANGE = json.loads(json.dumps(trendObject[str(trend_versionNum)]))
    print TREND_RANGE
    
    keyId = "AKIAJZ2N4UOI4TP4YBRQ"
    secret = "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"
    conn = boto.connect_sdb(keyId,secret)
    stock_list = ['MERVAL','MEXBOL','IBOV','CHILE65','COLCAP','CRSMBCT','BVPSBVPS','IGBVL','IBVC']
    update_records(conn,stock_list)
    
if __name__ == "__main__":
    
    main()

            
            
        