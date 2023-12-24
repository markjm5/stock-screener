import unittest
from common import get_zacks_us_companies
from common import set_zacks_balance_sheet_shares, set_zacks_earnings_surprises, set_zacks_product_line_geography
from common import set_finwiz_stock_data, set_stockrow_stock_data, set_yf_key_stats, set_zacks_peer_comparison
from common import dataframe_convert_to_numeric, get_logger
from common import set_earningswhispers_earnings_calendar, set_marketscreener_economic_calendar
from common import set_whitehouse_news, set_geopolitical_calendar, set_yf_price_action, set_price_action_ta
from common import set_todays_insider_trades, get_data, set_stlouisfed_data, set_yf_historical_data, calculate_etf_performance, calculate_annual_etf_performance
from common import set_ism_manufacturing, set_summary_ratios, set_ta_pattern_stocks, set_10y_rates, set_2y_rates, temp_load_excel_data_to_db
from common import calc_ir_metrics, set_country_credit_rating, set_us_treasury_yields, set_financialmodelingprep_dcf
from datetime import date
from config import YF_ETF_SERIES
#import chromedriver_autoinstaller as chromedriver
#chromedriver.install()

#YF_ETF_SERIES_TEST = [
#    'SLY'
#]

STLOUISFED_SERIES = [	
    'FEDFUNDS',	
]
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

df_interest_rates_10y = get_data(table="macro_ir_10y")           
df_interest_rates_10y = df_interest_rates_10y.sort_values('dt').fillna(method='ffill')           
df_ir = calc_ir_metrics(df_interest_rates_10y[["dt", "australia"]])

sheet_name = 'us_2y'

excel_file_path = '/data/temp_macro_data/us_2y.csv'

rename_cols = {
    'DATE':'dt',
#    'czech republic': 'czech_republic',
#    'hong kong': 'hong_kong',
#    'new zealand': 'new_zealand',
#    'south africa': 'south_africa',
#    'south korea': 'south_korea',
#    'u.k.': 'uk',
#    'u.s.':'us'
}

#conflict_cols = "dt"
#database_table = 'macro_ir_2y'
#todays_date = date.today()
#date_str = "%s%s" % (todays_date.strftime('%Y'), todays_date.strftime('%m'))

#y = 2023
#x = 10
#for y in range(2011, 2023):
#for x in range(1,13):
#    if(x < 10):
#        date_str = "%s0%s" % (y, x)
#    else:
#        date_str = "%s%s" % (y, x)
#
#    print(date_str)
#    df_treasury_yields = set_us_treasury_yields(date_str, logger)
#    print(df_treasury_yields)

class TestCommon(unittest.TestCase):

    #def test_get_us_treasury_yields(self):
    #    self.assertEqual(set_us_treasury_yields(date_str, logger),True)

    def test_set_financialmodelingprep_dcf(self):
        self.assertEqual(set_financialmodelingprep_dcf(df_tickers_alternate,logger),True)

    #def test_calc_ir_metrics(self):
    #    self.assertEqual(calc_ir_metrics(df_ir), True)

    #def test_set_country_credit_rating(self):
    #    self.assertEqual(set_country_credit_rating(logger), True)

    #def test_temp_load_excel_data_to_db(self):
    #    self.assertEqual(temp_load_excel_data_to_db(excel_file_path, sheet_name, database_table,rename_cols, conflict_cols), True)

    #def test_set_10y_rates(self):
    #    self.assertEqual(set_10y_rates(logger), True)

    #def test_set_2y_rates(self):
    #    self.assertEqual(set_2y_rates(logger), True)

    #def test_set_stlouisfed_data(self):
    #    self.assertEqual(set_stlouisfed_data(STLOUISFED_SERIES,logger), True)

    #def test_set_summary_ratios(self):
    #    self.assertEqual(set_summary_ratios(df_tickers,logger), True)

    #def test_set_price_action_ta(self):
    #    self.assertEqual(set_price_action_ta(df_tickers_alternate,logger), True)

    #def test_set_ta_pattern_stocks(self):
    #    self.assertEqual(set_ta_pattern_stocks(df_tickers_alternate,logger), True)

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
    #def test_set_stockrow_stock_data(self):
    #    self.assertEqual(set_stockrow_stock_data(df_tickers1, logger),True)
    
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