import unittest
from common import get_zacks_us_companies
from common import set_zacks_balance_sheet_shares, set_zacks_earnings_surprises, set_zacks_product_line_geography
from common import set_finwiz_stock_data, set_stockrow_stock_data, set_yf_key_stats, set_zacks_peer_comparison
from common import dataframe_convert_to_numeric, get_logger
from common import set_earningswhispers_earnings_calendar, set_marketscreener_economic_calendar
from common import set_whitehouse_news, set_geopolitical_calendar, set_yf_price_action, set_price_action_ta
from common import set_todays_insider_trades, get_data, set_stlouisfed_data, set_yf_historical_data, calculate_etf_performance, calculate_annual_etf_performance
from common import set_ism_manufacturing, set_summary_ratios
from config import YF_ETF_SERIES

#YF_ETF_SERIES_TEST = [
#    'SLY'
#]

#myset = set(YF_ETF_SERIES_NEW)
#newlist = list(myset)

#temp3 = []
#for element in newlist:
#    if element not in YF_ETF_SERIES:
#        temp3.append(element)
 
#print(temp3)

#import pdb; pdb.set_trace()

logger = get_logger()

df_tickers = get_zacks_us_companies()
#import pdb; pdb.set_trace()
#Ticker Sets used for testing
df_tickers1 = df_tickers.loc[df_tickers['Ticker'].isin(['NXE'])]
df_tickers2 = df_tickers.loc[df_tickers['Ticker'].isin(['ACWI'])]
df_tickers3 = df_tickers.loc[df_tickers['Ticker'].isin(['AIMC'])]
df_tickers4 = df_tickers.loc[df_tickers['Ticker'].isin(['TKAGY'])]
df_tickers5 = df_tickers.loc[df_tickers['Ticker'].isin(['ACGL'])]
df_tickers6 = df_tickers.loc[df_tickers['Ticker'].isin(['ADRNY'])]
df_tickers7 = df_tickers.loc[df_tickers['Ticker'].isin(['ADM'])]
df_tickers8 = df_tickers.loc[df_tickers['Ticker'].isin(['BC'])]
df_tickers9 = df_tickers.loc[df_tickers['Ticker'].isin(['BRK.A'])]

df_tickers_alternate = get_data(table="company") 
df_etf_historical_data = get_data(table="macro_yfhistoricaletfdata").reset_index(drop=True)
df_tickers_one_ticker = df_tickers_alternate.loc[df_tickers_alternate['symbol'].isin(['VSAT'])]

class TestCommon(unittest.TestCase):

    #def test_set_summary_ratios(self):
    #    self.assertEqual(set_summary_ratios(df_tickers,logger), True)

    #def test_set_price_action_ta(self):
    #    self.assertEqual(set_price_action_ta(df_tickers_alternate,logger), True)

    #def test_set_ism_manufacturing(self):
    #    self.assertEqual(set_ism_manufacturing(logger), True)

    #def test_set_yf_historical_data(self):
    #    self.assertEqual(set_yf_historical_data(YF_ETF_SERIES, logger),True)

    #def test_set_calculate_etf_performance(self):
    #    self.assertEqual(calculate_etf_performance(df_etf_historical_data, logger),True)

    #def test_set_calculate_annual_etf_performance(self):
    #    self.assertEqual(calculate_annual_etf_performance(df_etf_historical_data, logger),True)

    #def test_set_yf_price_action(self):
    #    self.assertEqual(set_yf_price_action(df_tickers_one_ticker, logger),True)
        
    #def test_set_earningswhispers_earnings_calendar(self):
    #    self.assertEqual(set_earningswhispers_earnings_calendar(df_tickers,logger),True)

    #def test_scrape_insider_trades(self):
    #    self.assertEqual(set_todays_insider_trades(logger),True)
    
    #def test_set_insider_trades_company(self):
    #    self.assertEqual(set_insider_trades_company(df_tickers1,logger),True)

    
    #def test_set_ta_pattern_stocks(self):
    #    self.assertEqual(set_yf_price_action(df_tickers, logger),True)
    

    #def test_scrape_table_marketscreener_economic_calendar(self):
    #    self.assertEqual(set_marketscreener_economic_calendar(logger),True)

                
    #def test_set_whitehouse_news(self):
    #    self.assertEqual(set_whitehouse_news(logger),True)
    

    #def test_set_geopolitical_calendar(self):
    #    self.assertEqual(set_geopolitical_calendar(logger),True)

    
    #Executor 1
    #def test_set_zacks_balance_sheet_shares(self):
    #    self.assertEqual(set_zacks_balance_sheet_shares(df_tickers1, logger),True)
       # self.assertEqual(set_zacks_balance_sheet_shares(df_tickers2, logger),False)

    
    #Executor 2
    #def test_set_zacks_earnings_surprises(self):
    #    self.assertEqual(set_zacks_earnings_surprises(df_tickers1, logger),True)
    
    #Executor 3
    #def test_set_zacks_product_line_geography(self):
    #    self.assertEqual(set_zacks_product_line_geography(df_tickers9, logger),True)
    
    #Executor 4
    #def test_set_finwiz_stock_data(self):
    #    self.assertEqual(set_finwiz_stock_data(df_tickers1, logger),True)
        #self.assertEqual(set_finwiz_stock_data(df_tickers6, logger),False)
    

    #Executor 5
    def test_set_stockrow_stock_data(self):
        self.assertEqual(set_stockrow_stock_data(df_tickers1, logger),True)
    
    #Executor 6
    #def test_set_yf_key_stats(self):
    #    self.assertEqual(set_yf_key_stats(df_tickers1, logger),True)

    #Executor 7
    #def test_set_zacks_peer_comparison(self):
    #    self.assertEqual(set_zacks_peer_comparison(df_tickers1, logger),True)
        #self.assertEqual(set_zacks_peer_comparison(df_tickers3, logger),False)
        #self.assertEqual(set_zacks_peer_comparison(df_tickers4, logger),False)
      
if __name__ == '__main__':
    unittest.main()