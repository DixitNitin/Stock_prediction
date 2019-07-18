from nsetools import Nse
from nsepy import get_history
import pandas as pd
from datetime import timedelta, date
from mysql import connector
import utility
import datetime


mydb = connector.connect(user='root', password='iiit123',
                             host='127.0.0.1', auth_plugin='mysql_native_password',
                             database='screener')
mycursor = mydb.cursor()

sql = "TRUNCATE TABLE daily_data"

mycursor.execute(sql)

sql = "INSERT INTO daily_data (timestamp, symbol, open, high, low, close, tottrdqty, totdelqty) VALUES (%s, %s,%s, %s,%s, %s,%s, %s)"

start_date = date(2015, 6, 6)
end_date = datetime.date.today()#date(2018, 10, 10)

#stock_list = ['SBIN', 'MARUTI', 'EXIDEIND', 'AUROLAB', 'AUROPHARMA', 'COLPAL', 'GRANULES','HINDUNILVR','JPASSOCIAT','KIRLOSBROS', 'KOTAKBANK','LUPIN','MARICO', 'ONGC', 'PTC', 'TATAMOTORS', 'TATASTEEL', 'VEDL']
stock_list = ['COLPAL']#['ACC','ADANIENT','ADANIPORTS','ADANIPOWER','AJANTPHARM','ALBK','AMARAJABAT','AMBUJACEM','APOLLOHOSP','APOLLOTYRE','ARVIND','ASHOKLEY','ASIANPAINT','AUROPHARMA','AXISBANK','BAJAJ-AUTO','BAJAJFINSV','BAJFINANCE','BALKRISIND','BANKBARODA','BANKINDIA','BATAINDIA','BEL','BEML','BERGEPAINT','BHARATFIN','BHARATFORG','BHARTIARTL','BHEL','BIOCON','BOSCHLTD','BPCL','BRITANNIA','CADILAHC','CANBK','CANFINHOME','CAPF','CASTROLIND','CEATLTD','CENTURYTEX','CESC','CGPOWER','CHENNPETRO','CHOLAFIN','CIPLA','COALINDIA','COLPAL','CONCOR','CUMMINSIND','DABUR','DALMIABHA','DCBBANK','DHFL','DISHTV','DIVISLAB','DLF','DRREDDY','EICHERMOT','ENGINERSIN','EQUITAS','ESCORTS','EXIDEIND','FEDERALBNK','GAIL','GLENMARK','GMRINFRA','GODFRYPHLP','GODREJCP','GODREJIND','GRANULES','GRASIM','GSFC','HAVELLS','HCC','HCLTECH','HDFC','HDFCBANK','HEROMOTOCO','HEXAWARE','HINDALCO','HINDPETRO','HINDUNILVR','HINDZINC','IBULHSGFIN','ICICIBANK','ICICIPRULI','IDBI','IDEA','IDFC','IDFCBANK','IFCI','IGL','INDIACEM','INDIANB','INDIGO','INDUSINDBK','INFIBEAM','INFRATEL','INFY','IOC','IRB','ITC','JETAIRWAYS','JINDALSTEL','JISLJALEQS','JPASSOCIAT','JSWSTEEL','JUBLFOOD','JUSTDIAL','KAJARIACER','KOTAKBANK','KPIT','KSCL','KTKBANK','L&TFH','LICHSGFIN','LT','LUPIN','M&M','M&MFIN','MANAPPURAM','MARICO','MARUTI','MCDOWELL-N','MCX','MFSL','MGL','MINDTREE','MOTHERSUMI','MRF','MRPL','MUTHOOTFIN','NATIONALUM','NBCC','NCC','NESTLEIND','NHPC','NIITTECH','NMDC','NTPC','OFSS','OIL','ONGC','ORIENTBANK','PAGEIND','PCJEWELLER','PEL','PETRONET','PFC','PIDILITIND','PNB','POWERGRID','PTC','PVR','RAMCOCEM','RAYMOND','RBLBANK','RCOM','RECLTD','RELCAPITAL','RELIANCE','RELINFRA','REPCOHOME','RPOWER','SAIL','SBIN','SHREECEM','SIEMENS','SOUTHBANK','SREINFRA','SRF','SRTRANSFIN','STAR','SUNPHARMA','SUNTV','SUZLON','SYNDIBANK','TATACHEM','TATACOMM','TATAELXSI','TATAGLOBAL','TATAMOTORS','TATAMTRDVR','TATAPOWER','TATASTEEL','TCS','TECHM','TITAN','TORNTPHARM','TORNTPOWER','TV18BRDCST','TVSMOTOR','UBL','UJJIVAN','ULTRACEMCO','UNIONBANK','UPL','VEDL','VGUARD','VOLTAS','WIPRO','WOCKPHARMA','YESBANK','ZEEL']
index_list = ['NIFTY 50']

def Calculate_Stocks():
	for company in stock_list:
		q = get_history(symbol=company,start=start_date,end=end_date)
		col = ['timestamp', 'Symbol', 'Open', 'High', 'Low', 'Close', 'Volume' ,'Deliverable Volume']
		df = pd.DataFrame(q)
		z=df.loc[start_date:end_date,col]
		#print (z)
		for date, data in z.iterrows():
			value = [date, data['Symbol'], data['Open'], data['High'], data['Low'], data['Close'], data['Volume'] , data['Deliverable Volume']]
			mycursor.execute(sql, value)
	
def Calculate_Indices():
	for index in index_list:
		q = get_history(symbol=index,start=start_date,end=end_date, index=True)
		col = ['Open', 'High', 'Low', 'Close']
		df = pd.DataFrame(q)
		z=df.loc[start_date:end_date,col]
		#print (z)
		for date, data in z.iterrows():
			value = [date, index, data['Open'], data['High'], data['Low'], data['Close'], 0 ,0]
			#print (value)
			mycursor.execute(sql, value)
	
Calculate_Stocks()
#Calculate_Indices()

mydb.commit()
	
mydb.close()
