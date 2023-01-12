import unittest
from unittest import suite
from binance import Client
from binance.exceptions import BinanceAPIException

def get_binance_exception(client) :
    try :
        status_code = client.response.status_code
        error_message = 'No error appearance'
    except BinanceAPIException as e:
        status_code = e.status_code
        error_message =  e.message
    return status_code,error_message

class Test_python_binance(unittest.TestCase):
    
    def __init__(self, testName, api_key,api_secret):
        super(Test_python_binance, self).__init__(testName)  # calling the super class init varies for different python versions.  This works for 2.7
        self.api_key = api_key
        self.api_secret = api_secret
        self.client = Client(self.api_key,self.api_secret)

    def test_connection(self):
        status = self.client.response.status_code
        self.assertEqual(status, 200)
        # check that s.split fails when the separator is not a string
        '''
        with self.assertRaises(BinanceAPIException):
            e_status_code,e_message = get_binance_exception(self.client)
            print("Status code of client is %s, with error message : %s" % (e_status_code,e_message))
        '''
    def test_get_histroical_kline(self) :
        res = self.client.get_klines(symbol='BTCUSDT', interval=Client.KLINE_INTERVAL_30MINUTE)
        res_flag = len(res) > 0
        self.assertTrue(res_flag, 'Length of return value > 0')
        

if __name__ == '__main__':
    api_key = 'x3ItBWRXVtaKTWXhko3zwP3RWoeidlKfi0evgDkIYlKIWvt0BiAot63FpKBmgaUU'
    api_secret = 'U791j2cPktcqLnK5kW7cTIrQ1jZOGwIgms4sTtgcD7bsHoPFRB8KPvx7Ncg07Lcv'
    
    suite = unittest.TestSuite()
    suite.addTest(Test_python_binance('test_connection',api_key,api_secret))
    suite.addTest(Test_python_binance('test_get_histroical_kline',api_key,api_secret))
    unittest.TextTestRunner(verbosity=2).run(suite)