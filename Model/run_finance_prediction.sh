#!/bin/sh

# This script should run after the stock value and news collecting scripts being finished
# To run the script, we need to indicate a config file, the daily stockindex file, the daily news file and the prediction date
# Then we need to configure the following parameters if need change:
# CLUSTER_PROBABILITY_PATH = ../Config/Data/FinalclusterProbability.json
# CLUSTER_CONTRIBUTION_PATH = ../Config/Data/FinalclusterContribution.json
# TERM_CONTRIBUTION_PATH = ../Config/Data/termContribution.json
# TREND_RANGE_FILE = ../Config/Data/trendRange.json
# VOCABULARY_FILE = d:/embers/financeModel/vocabulary.txt
# DB_FILE_PATH = ../Config/Data/embers.db
# ZMQ_PORT = tcp://*:30115


#Warning: just for test, I have put some test stock and news file in specified category, for production environment, we need to change them 
CONFIG_FILE='../Config/config.cfg'
DAILY_STOCK_FILE = '../Config/stock_2012-10-23.txt'
DAILY_NEWS_FILE = '../Config/Data/DailyNews/Bloomberg-News-2012-10-23'
PREDICT_DAY = '2012-10-23'

python ./prediction.py -c ${CONFIG_FILE} -d ${PREDICT_DAY} -s ${DAILY_STOCK_FILE} -n ${DAILY_NEWS_FILE}

