# Finance Model: Bayesian - Time serial model description
## 1. Data Structure(sqlite database table used in the model)
	 t_bloomberg_prices: 
		Used to store the daily raw stock prices
		columns: embers_id, type(currency | stock),name(stock index name),update_time,current_value,query_time,
		previous_close_value,post_date,source
	 t_enriched_bloomberg_prices:
		Used to store the daily enriched stock prices
		columns: embers_id, derived_from,type, name, post_date,operate_time,current_value,previous_close_value,
		one_day_change,change_percent,zscore30,zscore90,trend_type
	 t_daily_news:
		Storing the daily news
		columns: embers_id, title,author,post_time,post_date,content,stock_index,source,update_time,url
	 t_daily_enrichednews:
		Storing the enriched news,tokenize the content of the raw news
		columns: embers_id,derived_from, title, author, post_time,post_date,content, stock_index,sourece,update_time
	 t_surrogatedata:
		storing the surrogate data(the prediction of the trend type of the day)
		columns: embers_id, derived_from, shift-date,shift_type,confidence,strength,location,model,value_spectrum,
		confidence_isprobability, population, version, comments, description, operate_time
	 t_warningmessage:
		Storing all the predicion results of bayesian model (if the day not trige the sigma event, then the event code for
		that day will be set as "0000")
		columns: embers_id, derived_from, model, version, operate_time, event_type, confidence,
		confidence_isprobability,event_date,event_code, location, population, description, comments
	 s_holiday:
		Storing the holiday of the countries needed to be predicted
		columns: country, holiday_name, date
	 s_stock_country:
		Storing the association of the stock index and country
		column: stock_index, country


## 2. Model structure
	--bayesian_model
		--data
			--bayesian_model.conf (static file: input file for bayesian_model.py)
			--companyList.json (static file: input file for bloomberg_news_ingest.py)
			--embers.db (dynamic file: input file for bayesian_model.py, bloomberg_news_process.py and stock_process.py, should be downloaded from S3 each time run the model and then push back to S3)
			--bloombergNewsDownloaded.json (dynamic file: input for bloomberg_news_ingest.py should be downloaded from S3 each time run the model and then push back to S3)
			--trendRange.json (dynamic file: input file for stock_process.py and bayesian_model.py, should be downloaded from S3 each time run the model and then push back to S3)
		bayesian_model.py  (Used to make prediction and regenerate the old prediction)
		bloomberg_news_ingest.py (Used to ingest news from bloomberg web site )
		bloomberg_news_process.py (preprocess the raw news data)
		stock_process.py (process the raw stock prices data)
		calculator.py (common tools to compute the standard deviation and zscore)

## 3. Usage	
	Before talking about the usage of bayesian - time serial model, i will breifly describe the data flow
	in the model.
	1> Run bloomberg_prices_ingest (already resident in EC2 server) to get the stock prices from bloomberg site, this program 
	will push the raw stock prices to ZMQ and then store them as a daily file in S3 called: stock-yyyy-mm-dd.txt(each line is a record of stock price)
	2> Run bloomberg_news_ingest program: after getting news from web, it will push each news as a message to ZMQ, and then store the news
	as file bloombergnews-yyyy-mm-dd.txt in S3, each line would be one news data(json format)
	3> Run stock_process program: it takes the daily stock prices file(stored in S3) as input and does steps as below:
		3.1> Store the daily stock records in table: t_bloomberg_prices
		3.2> Compute the one_day_change,change_percent,zscore30,zscore90 and the trend type for each record
		3.3> Store the results as enriched data in table: t_enriched_bloomberg_prices
		3.4> Push the enriched data into ZMQ
	4> Run the bloomberg_news_process program: it takes the daily news file(stored in S3) as input and do following steps:
		4.1> Store the raw news in table t_daily_news
		4.2> Tokenize the content of the news and store this as enriched news in table: t_daily_enrichednews
		4.3> Push the enriched news as message to ZMQ
	5> Run the bayesian_model program to make prediction or regenerate the old prediction, it follows the below steps:
		5.1> Get the date to be predicted from input and check if it is a holiday or weekend. If yes, then skip this day, otherwise to do the following steps.
		The holiday information stored in table: s_holiday.
		5.2> if the parameter date to be predicted comes from --rg paramenter, it means to regenerate the old prediction. Then 
		the config object will be set as same as the one used to make the old prediction. If the date comes from --d pramete, which means it is a normal preidcion. Then
		the config object will be load as the latest version from config file.
		5.3> Query the past 3 day's enriched stock prices from table: t_enriched_bloomberg_prices and compute the stock contribution
		5.4> Query the past 3 day's enriched news from table: t_daily_enrichednews and compute the news contribution
		5.5> Apply the bayesian algorithm to make prediction of trend type of the day to be predicted, which is surrogate data. Store the surrogate data in table t_surrogatedata
		and push it to ZMQ
		5.6> Use the surrogate data to determine if it would trige a sigma event and store the result as warningmessage in table: t_warningmess. If it is not a sigma event, the event_code
		will be set as "0000" to distinguished from the real warning message. If it is a warnning message, it will be pushed to ZMQ.
	
	Usage detail:
	Schedule:
		Set bloomberg_news_ingest.py as crontab run at 9:00pm each day. 
		After the bloomberg_news_ingest and bloomberg_prices_ingest finished, it is time to run bloomberg_news_process.py and stock_process.py
		Finally, after the bloomberg_news_process and stock_process completed, run bayesian_model.py to make prediction
		
	bloombert_news_ingest.py: 
		Tips: Before download the detail content of each news, bloomberg_news_ingest will check if the news already in BloombergNewsDownloaded, if yes then skip
		this news. BloombergNewsDownloaded.json will be updated each time, so it should be push back to S3 to keep updated.
		
		ap.add_argument('-c',dest="f_company_list",metavar="COMPANY_LIST",default="./data/companyList.json",type=str,nargs='?',help='the company list file')
		ap.add_argument('-d',dest="f_downloaded",metavar="ALREADY DOWNLOADED NEWS",default="./data/BloombergNewsDownloaded.json", type=str,nargs='?',help="The already downloaded news")
		ap.add_argument('-z',dest="port",metavar="ZMQ PORT",default="tcp://*:30115",type=str,nargs="?",help="The zmq port")
		
		eg: bloombert_news_ingest.py -c ./data/companyList.json -d ./data/BloombergNewsDownloaded.json -z tcp://*:30115
	
	bloomberg_news_process.py:
		ap.add_argument('-z',dest="port",metavar="ZMQ PORT",default="tcp://*:30115",type=str,nargs="?",help="The zmq port")
		ap.add_argument('-db',dest="db_file",metavar="Database",type=str,help='The stock price file')
		ap.add_argument('-f',dest="bloomberg_news_file",metavar="BLOOMBERG_NEWS",type=str,help='The daily BLOOMBERG NEWS file')
		
		eg: bloomberg_news_process.py -z tcp://*:30115 -db ./embers.db -f ./bloomberg-2012-11-02.txt
	
	stock_process.py
		Tips: trendRange.json will be updated after running the stock_process.py. So it also should be pushed back to S3 to keep updated.
		ap.add_argument('-f',dest="bloomberg_price_file",metavar="STOCK PRICE",type=str,help='The stock price file')
		ap.add_argument('-t',dest="trend_file",metavar="TREND RANGE FILE",type=str,help='The trend type range')
		ap.add_argument('-db',dest="db_file",metavar="Database",type=str,help='The stock price file')
		ap.add_argument('-z',dest="port",metavar="ZMQ PORT",default="tcp://*:30115",type=str,nargs="?",help="The zmq port")
		
		eg: stock_process.py -z tcp://*:30115 -db ./embers.db -f ./stock-2012-11-02.txt -t ./trendRange.json
	
	bayesian_model.py
		ap.add_argument('-c',dest="model_cfg",metavar="MODEL CFG",default="./data/bayesian_model.cfg",type=str,nargs='?',help='the config file')
		ap.add_argument('-tf',dest="trend_file",metavar="TREND RANGE FILE",default="./data/trendRange.json", type=str,nargs='?',help="The trend range file")
		ap.add_argument('-z',dest="port",metavar="ZMQ PORT",default="tcp://*:30115",type=str,nargs="?",help="The zmq port")
		ap.add_argument('-db',dest="db_file",metavar="Database",type=str,help='The sqlite database file')
		default_day = datetime.strftime(datetime.now() + timedelta(days =1),"%Y-%m-%d")
		ap.add_argument('-d',dest="predict_date",metavar="PREDICT DATE",type=str,default=default_day,nargs="?",help="The day to be predicted")
		ap.add_argument('-s',dest="stock_list",metavar="Stock List",type=str,nargs="+",help="The list of stock to be predicted")
		ap.add_argument('-rg',dest="rege_date",metavar="Regenerate Date",type=str,help="The date need to be regerated")
		
		eg: bayesian_model.py -z tcp://*:30115 -db ./embers.db -tf ./trendRange.json
		
