import unittest
from common import get_zacks_us_companies
from common import set_zacks_balance_sheet_shares, set_zacks_earnings_surprises, set_zacks_product_line_geography
from common import set_finwiz_stock_data, set_stockrow_stock_data, set_yf_key_stats, set_zacks_peer_comparison
from common import dataframe_convert_to_numeric, get_logger
from common import scrape_table_earningswhispers_earnings_calendar, scrape_table_marketscreener_economic_calendar

logger = get_logger()

df_tickers = get_zacks_us_companies()

#Ticker Sets used for testing
df_tickers1 = df_tickers.loc[df_tickers['Ticker'].isin(['AAPL'])]
df_tickers2 = df_tickers.loc[df_tickers['Ticker'].isin(['ACWI'])]
df_tickers3 = df_tickers.loc[df_tickers['Ticker'].isin(['AIMC'])]
df_tickers4 = df_tickers.loc[df_tickers['Ticker'].isin(['TKAGY'])]
df_tickers5 = df_tickers.loc[df_tickers['Ticker'].isin(['ACGL'])]
df_tickers6 = df_tickers.loc[df_tickers['Ticker'].isin(['ADRNY'])]
df_tickers7 = df_tickers.loc[df_tickers['Ticker'].isin(['ADM'])]

class TestCommon(unittest.TestCase):
    
    def test_scrape_table_earningswhispers_earnings_calendar(self):
        self.assertEqual(scrape_table_earningswhispers_earnings_calendar(df_tickers, logger),True)

    def test_scrape_table_marketscreener_economic_calendar(self):
        self.assertEqual(scrape_table_marketscreener_economic_calendar(logger),True)

    """    
    #Executor 1
    def test_set_zacks_balance_sheet_shares(self):
        self.assertEqual(set_zacks_balance_sheet_shares(df_tickers1, logger),True)
        self.assertEqual(set_zacks_balance_sheet_shares(df_tickers2, logger),False)

    #Executor 2
    def test_set_zacks_earnings_surprises(self):
        self.assertEqual(set_zacks_earnings_surprises(df_tickers1, logger),True)

    #Executor 3
    def test_set_zacks_product_line_geography(self):
        self.assertEqual(set_zacks_product_line_geography(df_tickers1, logger),True)
    
    #Executor 4
    def test_set_finwiz_stock_data(self):
        self.assertEqual(set_finwiz_stock_data(df_tickers1, logger),True)
        self.assertEqual(set_finwiz_stock_data(df_tickers6, logger),False)
    
    #Executor 5
    def test_set_stockrow_stock_data(self):
        self.assertEqual(set_stockrow_stock_data(df_tickers5, logger),True)
        self.assertEqual(set_stockrow_stock_data(df_tickers6, logger),False)
        self.assertEqual(set_stockrow_stock_data(df_tickers7, logger),True)
    
    #Executor 6
    def test_set_yf_key_stats(self):
        self.assertEqual(set_yf_key_stats(df_tickers1, logger),True)

    #Executor 7
    def test_set_zacks_peer_comparison(self):
        self.assertEqual(set_zacks_peer_comparison(df_tickers1, logger),True)
        self.assertEqual(set_zacks_peer_comparison(df_tickers3, logger),False)
        self.assertEqual(set_zacks_peer_comparison(df_tickers4, logger),False)
    """

    #def test_dataframe_convert_to_numeric(self):
    # TODO: write test cases for this function
    #    pass
    

if __name__ == '__main__':
    unittest.main()