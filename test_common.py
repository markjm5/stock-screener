import unittest
from common import get_zacks_us_companies
from common import get_zacks_balance_sheet_shares, get_zacks_earnings_surprises, get_zacks_product_line_geography
from common import get_finwiz_stock_data, get_stockrow_stock_data, get_yf_key_stats, get_zacks_peer_comparison
from common import dataframe_convert_to_numeric, get_logger

logger = get_logger()

df_tickers = get_zacks_us_companies()

#Ticker Sets used for testing
df_tickers1 = df_tickers.loc[df_tickers['Ticker'].isin(['AAPL'])]
df_tickers2 = df_tickers.loc[df_tickers['Ticker'].isin(['ACWI'])]
df_tickers3 = df_tickers.loc[df_tickers['Ticker'].isin(['AIMC'])]
df_tickers4 = df_tickers.loc[df_tickers['Ticker'].isin(['TKAGY'])]
df_tickers5 = df_tickers.loc[df_tickers['Ticker'].isin(['ACGL'])]

class TestCommon(unittest.TestCase):

    #Executor 1
    def test_get_zacks_balance_sheet_shares(self):
        self.assertEqual(get_zacks_balance_sheet_shares(df_tickers1, logger),True)
        self.assertEqual(get_zacks_balance_sheet_shares(df_tickers2, logger),False)

    #Executor 2
    def test_get_zacks_earnings_surprises(self):
        self.assertEqual(get_zacks_earnings_surprises(df_tickers1, logger),True)

    #Executor 3
    def test_get_zacks_product_line_geography(self):
        self.assertEqual(get_zacks_product_line_geography(df_tickers1, logger),True)

    #Executor 4
    def test_get_finwiz_stock_data(self):
        self.assertEqual(get_finwiz_stock_data(df_tickers1, logger),True)
    

    #Executor 5
    def test_get_stockrow_stock_data(self):
        self.assertEqual(get_stockrow_stock_data(df_tickers5, logger),True)

    
    #Executor 6
    def test_get_yf_key_stats(self):
        self.assertEqual(get_yf_key_stats(df_tickers1, logger),True)

    #Executor 7
    def test_get_zacks_peer_comparison(self):
        self.assertEqual(get_zacks_peer_comparison(df_tickers1, logger),True)
        self.assertEqual(get_zacks_peer_comparison(df_tickers3, logger),False)
        self.assertEqual(get_zacks_peer_comparison(df_tickers4, logger),False)

    #def test_dataframe_convert_to_numeric(self):
    # TODO: write test cases for this function
    #    pass
    

if __name__ == '__main__':
    unittest.main()