# Functions in this module will only be common for the whole probject
import ConfigParser
import sqlite3 as lite

# If the path of config.cfg is not indicated, then the default path is in ../Config
cfgFileName = '../Config/config.cfg'

# Before invoke the method in common module, we should init it firstly

def init(cfgPath):
    global cfgFileName
    cfgFileName = cfgPath
    
def get_configuration( section_name, configuration):
    global cfgFileName
    
    config = ConfigParser.ConfigParser()
    with open(cfgFileName, 'r' ) as cfgFile:
        config.readfp( cfgFile )
    configuration = config.get( section_name, configuration )
    return configuration

def getLocationByStockIndex(stockIndex):
    con = getDBConnection()
    cur = con.cursor()
    sql = "select country from s_stock_country where stock_index=?"
    cur.execute(sql,(stockIndex,))
    result = cur.fetchone()
    country = result[0]
    return country

def getDBConnection():
    dbPath = get_configuration("info","DB_FILE_PATH")
    con = lite.connect(dbPath)
    con.text_factory = str
    return con
