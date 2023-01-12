import psycopg2
from datetime import datetime, timedelta
from binance import Client
from airflow import DAG
from airflow.utils.db import provide_session # type: ignore
from airflow.models import XCom
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator  # type: ignore
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator  # type: ignore
from airflow.decorators import task
from airflow.operators.python import get_current_context

#### parse python-binance data
class python_binance_scrapper():
    def __init__(self, api_key,api_secret):
        self.api_key = api_key
        self.api_secret = api_secret
        self.client = Client(self.api_key,self.api_secret)

    def test_connection(self,ti):
        status = self.client.response.status_code
        print(status)
        ti.xcom_push(key="binance_status_code", value=status)
        
    def get_histroical_kline(self,ti) :
        token_list = ['BTC','ETH','BNB','MATIC','AVAX','ADA','ATOM','DOT','APT','DOGE']
        symbol_list = [x + 'USDT' for x in token_list]
        kline_list = []
        for symbol  in symbol_list :
            res = self.client.get_historical_klines(symbol=symbol,interval=Client.KLINE_INTERVAL_1HOUR,start_str="1 hour ago UTC")
            kline = format_binance_return(res[-1])
            kline['token'] = symbol
            kline_list.append(kline)
        ti.xcom_push(key='kline',value=kline_list)
#### change timestamp
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

### insert data
def insert_kline_data(ti) :
    conn = psycopg2.connect("dbname='airflow' user='airflow' host='postgres' password='airflow'")
    cur = conn.cursor()
    kline = ti.xcom_pull(key = 'kline')
    sql_command = """INSERT INTO price VALUES 
    (%(token)s,%(opentime)s,%(open)s,%(high)s,%(low)s,%(close)s,%(volume)s,%(closetime)s,%(percentage_change)s)"""
    cur.executemany(sql_command, kline)
    conn.commit()
    conn.close()
    
#### airflow setting
def slack_notification(context):
    slack_msg = """
            :red_circle: Task Failed. 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
            task=context.get('task_instance').task_id,
            dag=context.get('task_instance').dag_id,
            ti=context.get('task_instance'),
            exec_date=context.get('execution_date'),
            log_url=context.get('task_instance').log_url,
        )
    failed_alert = SlackWebhookOperator(
        task_id='slack_notification',
        http_conn_id='Slack_API',
        message=slack_msg)
    return failed_alert.execute(context=context)

default_args = {
'owner'                 : 'bruce',
'description'           : 'Parse data into local PostgreSQL',
'start_date'            : datetime(2022,12,1),
'depend_on_past'        : False,
'email_on_failure'      : False,
'email_on_retry'        : False,
'retries'               : 1,
'on_failure_callback'   : slack_notification,
'retry_delay'           : timedelta(seconds=300)
}

with DAG('parse_data_into_PostgreSQL', default_args=default_args, schedule_interval='@hourly') as dag:
    
    test_scrapper = python_binance_scrapper(api_key='x3ItBWRXVtaKTWXhko3zwP3RWoeidlKfi0evgDkIYlKIWvt0BiAot63FpKBmgaUU',
                                            api_secret='U791j2cPktcqLnK5kW7cTIrQ1jZOGwIgms4sTtgcD7bsHoPFRB8KPvx7Ncg07Lcv')
    
    @task.branch(task_id='check_binance_connection')
    def check_binance_connection(scrapper):
        context = get_current_context()
        status_code = scrapper.test_connection(context['ti'])
        print(status_code)
        if status_code == 200 :
            return "python_binance_price_scrapper"
        else :
            return "python_binance_price_scrapper"
                
    get_binance_histroical_klines = PythonOperator(
        task_id = "python_binance_price_scrapper",
        python_callable=test_scrapper.get_histroical_kline,
        dag=dag
    )
        
    @task.branch(task_id='check_price_table_exist')
    def check_table_exist():
        conn = psycopg2.connect("dbname='airflow' user='airflow' host='postgres' password='airflow'")
        cur = conn.cursor()
        cur.execute("select exists (select from pg_tables where schemaname = 'public' and tablename = 'price');")
        res = cur.fetchall()
        flag = res[0][0]
        context = get_current_context()
        context['ti'].xcom_push(key="table_exist",value = flag)
        if flag == True:
            return "insert_binance_kline" 
        else :
            return "create_price_table"
    
    create_price_table = PostgresOperator(
        task_id="create_price_table",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS price (
            token VARCHAR NOT NULL,
            open_time TIMESTAMP NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL NOT NULL,
            close_time TIMESTAMP NOT NULL,
            percentage_change REAL NOT NULL);
          """,
          dag=dag
    )
        
    insert_binance_kline = PythonOperator(
        task_id = "insert_binance_kline",
        python_callable=insert_kline_data,
        trigger_rule='none_failed',
        dag=dag
    )
    # remove xcom variable after dags operation finished
    @provide_session
    def cleanup_xcom(session=None, **context):
        dag = context["dag"]
        dag_id = dag._dag_id 
        # It will delete all xcom of the dag_id
        session.query(XCom).filter(XCom.dag_id == dag_id).delete() # type: ignore

    clean_xcom = PythonOperator(
        task_id="clean_xcom",
        python_callable = cleanup_xcom,
        provide_context=True, 
        # dag=dag
    )

    connection_status = check_binance_connection(test_scrapper)
    check_price_table_exist = check_table_exist()
    connection_status >> get_binance_histroical_klines >> check_price_table_exist >> insert_binance_kline >> clean_xcom # type: ignore
    connection_status >> get_binance_histroical_klines >> check_price_table_exist >> create_price_table >> insert_binance_kline >> clean_xcom # type: ignore
    