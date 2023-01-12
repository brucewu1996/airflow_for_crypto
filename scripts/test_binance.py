from binance import Client


def main() :
    api_key = 'x3ItBWRXVtaKTWXhko3zwP3RWoeidlKfi0evgDkIYlKIWvt0BiAot63FpKBmgaUU'
    api_secret = 'U791j2cPktcqLnK5kW7cTIrQ1jZOGwIgms4sTtgcD7bsHoPFRB8KPvx7Ncg07Lcv'
    client= Client(api_key,api_secret)
    
    res = client.get_klines(symbol='BTCUSDT', interval=Client.KLINE_INTERVAL_30MINUTE)
    
if __name__ == '__main__' :
    main()

