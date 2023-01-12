import psycopg2
from datetime import datetime,timedelta
from binance import Client

def postgres_table_exist(conn,table_name):
    cur = conn.cursor()
    cur.execute("select exists (select from pg_tables where schemaname = 'public' and tablename = '%s');" % table_name)
    res = cur.fetchall()
    flag = res[0][0]
    return flag

def create_table(conn,table_name):
    cur = conn.cursor()
    sql_command ="""
            CREATE TABLE IF NOT EXISTS %s (
            token VARCHAR NOT NULL,
            open_time TIMESTAMP NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL NOT NULL,
            close_time TIMESTAMP NOT NULL,
            percentage_change REAL NOT NULL);
          """ % table_name
    cur.execute(sql_command)
    conn.commit()
    
def insert_data_into_table(data,conn) :
    '''
    data : list; contain multiple kline dict data
    '''
    cur = conn.cursor()
    sql_command = """INSERT INTO test_price VALUES 
    (%(token)s,%(opentime)s,%(open)s,%(high)s,%(low)s,%(close)s,%(volume)s,%(closetime)s,%(percentage_change)s)"""
    cur.executemany(sql_command, data)
    conn.commit()
    conn.close()
    
### binance function
#### parse python-binance data
def format_binance_return(res,delta=timedelta(hours=8)) :
    '''
    res : list; return from python-binance 
    '''
    keys = ['opentime','open','high','low','close','volume','closetime','quote_volume',
            'ntrades','taker_buy_volume','taker_buy_quote_volume','ignore']
    kline = dict(zip(keys[:7],res[:7]))
    kline['opentime'] = (datetime.fromtimestamp(kline['opentime'] / 1000) + delta).strftime('%Y-%m-%d %H:%M:%S')
    kline['closetime'] =(datetime.fromtimestamp(kline['closetime'] / 1000) + delta).strftime('%Y-%m-%d %H:%M:%S')
    numeric_key = ['open','high','low','close','volume']
    for k in numeric_key :
        kline[k] = round(float(kline[k]),2)
    kline['percentage_change'] = round(100 * ((kline['high'] - kline['low']) / kline['open']),2)
    return kline

class python_binance_scrapper():
    def __init__(self, api_key,api_secret):
        self.api_key = api_key
        self.api_secret = api_secret
        self.client = Client(self.api_key,self.api_secret)

    def test_connection(self,ti):
        status = self.client.response.status_code
        print(status)
        ti.xcom_push(key="binance_status_code", value=status)
        
    def get_histroical_kline(self) :
        res = self.client.get_historical_klines(symbol='BTCUSDT',interval=Client.KLINE_INTERVAL_1HOUR,start_str="1 hour ago UTC")
        kline = format_binance_return(res[-1])
        return kline       
    

def main() :
    scrapper = python_binance_scrapper(api_key='x3ItBWRXVtaKTWXhko3zwP3RWoeidlKfi0evgDkIYlKIWvt0BiAot63FpKBmgaUU',
                                       api_secret='U791j2cPktcqLnK5kW7cTIrQ1jZOGwIgms4sTtgcD7bsHoPFRB8KPvx7Ncg07Lcv')
    
    conn = psycopg2.connect("dbname='airflow' user='airflow' host='postgres' password='airflow'")
    data = scrapper.get_histroical_kline()
    data['token'] = 'BTCUSDT'
    ps_table_exist = postgres_table_exist(conn,'test_price')
    if ps_table_exist == False :
        create_table(conn,'test_price')
        
    data_list = [data,data]
    insert_data_into_table(data_list,conn)
    conn.close()
    
if __name__ == '__main__' :
    main()

