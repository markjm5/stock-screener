import unittest
from common import get_zacks_us_companies
from common import get_zacks_balance_sheet_shares, get_zacks_earnings_surprises, get_zacks_product_line_geography
from common import get_finwiz_stock_data, get_stockrow_stock_data, get_yf_key_stats, get_zacks_peer_comparison

df_tickers = get_zacks_us_companies()
#df_tickers1 = df_tickers.loc[df_tickers['Ticker'].isin(['AAPL','ACWI'])]

class TestCommon(unittest.TestCase):

    #Executor 1
    def test_get_zacks_balance_sheet_shares(self):
        df_tickers1 = df_tickers.loc[df_tickers['Ticker'].isin(['AAPL'])]
        self.assertEqual(get_zacks_balance_sheet_shares(df_tickers1),True)

        df_tickers1 = df_tickers.loc[df_tickers['Ticker'].isin(['ACWI'])]
        self.assertEqual(get_zacks_balance_sheet_shares(df_tickers1),False)

    #Executor 2
    def test_get_zacks_earnings_surprises(self):
        df_tickers1 = df_tickers.loc[df_tickers['Ticker'].isin(['AAPL'])]
        self.assertEqual(get_zacks_earnings_surprises(df_tickers1),True)

    #Executor 3
    def test_get_zacks_product_line_geography(self):
        df_tickers1 = df_tickers.loc[df_tickers['Ticker'].isin(['AAPL'])]
        self.assertEqual(get_zacks_product_line_geography(df_tickers1),True)

    #Executor 4
    def test_get_finwiz_stock_data(self):
        df_tickers1 = df_tickers.loc[df_tickers['Ticker'].isin(['AAPL'])]
        self.assertEqual(get_finwiz_stock_data(df_tickers1),True)

    #Executor 5
    def test_get_stockrow_stock_data(self):
        df_tickers1 = df_tickers.loc[df_tickers['Ticker'].isin(['AAPL'])]
        self.assertEqual(get_stockrow_stock_data(df_tickers1),True)

    #Executor 6
    def test_get_yf_key_stats(self):
        df_tickers1 = df_tickers.loc[df_tickers['Ticker'].isin(['AAPL'])]
        self.assertEqual(get_yf_key_stats(df_tickers1),True)

    #Executor 7
    def test_get_zacks_peer_comparison(self):
        df_tickers1 = df_tickers.loc[df_tickers['Ticker'].isin(['AAPL'])]
        self.assertEqual(get_zacks_peer_comparison(df_tickers1),True)

        df_tickers1 = df_tickers.loc[df_tickers['Ticker'].isin(['AIMC'])]
        self.assertEqual(get_zacks_peer_comparison(df_tickers1),False)

if __name__ == '__main__':
    unittest.main()