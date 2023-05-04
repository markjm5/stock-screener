import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import streamlit as st
import sys
import requests
import time
import glob
import os
import os.path
import pandas as pd
import json
import copy
import re
import math
import yfinance as yf
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from datetime import date
from datetime import datetime as dt
from bs4 import BeautifulSoup
import psycopg2, psycopg2.extras
import config
import logging

isWindows = False

if(sys.platform == 'win32'):
  isWindows = True

############################
# Data Retrieval Functions #
############################

def get_page(url):
  # When website blocks your request, simulate browser request: https://stackoverflow.com/questions/56506210/web-scraping-with-python-problem-with-beautifulsoup
  header={'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.0 Safari/537.36'}
  page = requests.get(url=url,headers=header)

  try:
      page.raise_for_status()
  except requests.exceptions.HTTPError as e:
      # Whoops it wasn't a 200
      raise Exception("Http Response (%s) Is Not 200: %s" % (url, str(page.status_code)))

  return page

def get_page_selenium(url):

  #Selenium Browser Emulation Tool
  chrome_options = Options()
  chrome_options.add_argument("--headless")
  chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows Phone 10.0; Android 4.2.1; Microsoft; Lumia 640 XL LTE) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Mobile Safari/537.36 Edge/12.10166")


  driver = webdriver.Chrome(ChromeDriverManager().install(),options=chrome_options)
  driver.get(url)
  driver.implicitly_wait(10)  
  time.sleep(5)
  html = driver.page_source
  driver.close()
  
  return html


def get_yf_historical_stock_data(ticker, interval, start, end):
  data = yf.download(  # or pdr.get_data_yahoo(...
    # tickers list or string as well
    tickers = ticker,

    start=start, 
    end=end, 

    # use "period" instead of start/end
    # valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
    # (optional, default is '1mo')
    period = "ytd",

    # fetch data by interval (including intraday if period < 60 days)
    # valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
    # (optional, default is '1d')
    interval = interval,

    # group by ticker (to access via data['SPY'])
    # (optional, default is 'column')
    group_by = 'ticker',

    # adjust all OHLC automatically
    # (optional, default is False)
    auto_adjust = True,

    # download pre/post regular market hours data
    # (optional, default is False)
    prepost = True,

    # use threads for mass downloading? (True/False/Integer)
    # (optional, default is True)
    threads = True,

    # proxy URL scheme use use when downloading?
    # (optional, default is None)
    proxy = None
  )

  df_yf = data.reset_index()
  df_yf = df_yf.rename(columns={"Date": "DATE"})

  return df_yf

def get_yf_analysis(ticker):
  company = yf.Ticker(ticker)
  #import pdb; pdb.set_trace()
  # get stock info
  company.info

  # get historical market data
  hist = company.history(period="max")

  # show actions (dividends, splits)
  company.actions

  # show dividends
  company.dividends

  # show splits
  company.splits

  # show financials
  company.financials
  company.quarterly_financials

  # show major holders
  company.major_holders

  # show institutional holders
  company.institutional_holders

  # show balance sheet
  company.balance_sheet
  company.quarterly_balance_sheet

  # show cashflow
  company.cashflow
  company.quarterly_cashflow

  # show earnings
  company.earnings
  company.quarterly_earnings

  # show sustainability
  company.sustainability

  # show analysts recommendations
  company.recommendations

  # show next event (earnings, etc)
  company.calendar

  # show all earnings dates
  company.earnings_dates

  # show ISIN code - *experimental*
  # ISIN = International Securities Identification Number
  company.isin

  # show options expirations
  company.options

  # show news
  company.news

  # get option chain for specific expiration
  opt = company.option_chain('YYYY-MM-DD')
  # data available via: opt.calls, opt.puts

def get_yf_key_stats(df_tickers, logger):
  success = False
  for index, row in df_tickers.iterrows():
    ticker = row['Ticker']  

    logger.info(f'Getting YF Key Stats for {ticker}')

    df_company_data = pd.DataFrame()
    url = "https://finance.yahoo.com/quote/%s/key-statistics?p=%s" % (ticker, ticker)
    try:
      page = get_page(url)
      
      soup = BeautifulSoup(page.content, 'html.parser')

      tables = soup.find_all('table')
      statsDict = {}
      #try:
      for table in tables:
        table_rows = table.find_all('tr', recursive=True)
        emptyDict = {}

        #Get rows of data.
        for tr in table_rows:
          tds = tr.find_all('td')
          boolKey = True
          keyValueSet = False

          for td in tds:
              if boolKey:
                  key = td.text.strip()
                  boolKey = False                
              else:
                  value = td.text.strip()
                  boolKey = True
                  keyValueSet = True                

              if keyValueSet:
                  emptyDict[key] = value
                  keyValueSet = False
        statsDict.update(emptyDict)
    except Exception as e:
      #For some reason YF page did not load. Continue on and handle the exception below
      pass
    #import pdb; pdb.set_trace()
    try:
      df_company_data.loc[ticker, 'MARKET_CAP'] = statsDict['Market Cap (intraday)']
      df_company_data.loc[ticker, 'EV'] = statsDict['Enterprise Value']
      df_company_data.loc[ticker, 'AVG_VOL_3M'] = statsDict['Avg Vol (3 month) 3']
      df_company_data.loc[ticker, 'AVG_VOL_10D'] = statsDict['Avg Vol (10 day) 3']
      df_company_data.loc[ticker, '50_DAY_MOVING_AVG'] = statsDict['50-Day Moving Average 3']
      df_company_data.loc[ticker, '200_DAY_MOVING_AVG'] = statsDict['200-Day Moving Average 3']
      df_company_data.loc[ticker, 'EV_REVENUE'] = statsDict['Enterprise Value/Revenue']
      df_company_data.loc[ticker, 'EV_EBITDA'] = statsDict['Enterprise Value/EBITDA']
      df_company_data.loc[ticker, 'PRICE_BOOK'] = statsDict['Price/Book (mrq)']

      df_company_data = dataframe_convert_to_numeric(df_company_data, '50_DAY_MOVING_AVG', logger)
      df_company_data = dataframe_convert_to_numeric(df_company_data, '200_DAY_MOVING_AVG', logger)

      logger.info(f'Successfully Retrieved YF Key Stats for {ticker}')
    except KeyError as e:
      logger.info(f'Did not return YF stock data for {ticker}')      

    # get ticker cid
    cid = sql_get_cid(ticker)
    if(cid):
      # write records to database
      rename_cols = {"50_DAY_MOVING_AVG": "MOVING_AVG_50D", "200_DAY_MOVING_AVG": "MOVING_AVG_200D"}
      add_col_values = {"cid": cid}
      conflict_cols = "cid"

      success = sql_write_df_to_db(df_company_data, "CompanyMovingAverage", rename_cols, add_col_values, conflict_cols)

  return success

def get_yf_price_action(ticker):
    url_yf_modules = "https://query2.finance.yahoo.com/v10/finance/quoteSummary/%s?modules=summaryProfile,financialData,summaryDetail,price,defaultKeyStatistics" % (ticker)
    json_yf_modules = json.loads(get_page(url_yf_modules).content)

    return json_yf_modules

def write_zacks_ticker_data_to_db(df_tickers, logger):
  #create new df using columns from old df
  df_tickers_updated = pd.DataFrame(columns=df_tickers.columns)
  connection, cursor = sql_open_db()

  for index, row in df_tickers.iterrows():
      symbol = row["Ticker"]
      company = row["Company Name"] 
      sector = row["Sector"] 
      industry = row["Industry"] 
      exchange = row["Exchange"] 

      # Get market cap as well, because we want to use it in the earnings calendar
      market_cap = row["Market Cap (mil)"] 
      exchanges = ['NYSE', 'NSDQ']
      # Check that Company is not empty, and only add to the master ticker file if company is not empty
      if(company != '' and exchange in exchanges):
          try:
              shares_outstanding = float(row["Shares Outstanding (mil)"])
              shares_outstanding = shares_outstanding *1000000                    
          except Exception as e:
              shares_outstanding = 0
          logger.info(f'Loading Zacks stock data for {symbol}')
      try:
          # Write to database        
          sqlCmd = """INSERT INTO company (symbol, company_name, sector, industry, exchange, market_cap, shares_outstanding) VALUES
              ('{}','{}','{}','{}','{}','{}','{}')
              ON CONFLICT (symbol)
              DO
                  UPDATE SET company_name=excluded.company_name,sector=excluded.sector,industry=excluded.industry,exchange=excluded.exchange,market_cap=excluded.market_cap,shares_outstanding=excluded.shares_outstanding;
          """.format(sql_escape_str(symbol), sql_escape_str(company), sql_escape_str(sector), sql_escape_str(industry), sql_escape_str(exchange), market_cap, shares_outstanding)
          cursor.execute(sqlCmd)

          #Make the changes to the database persistent
          connection.commit()
          df_tickers_updated = df_tickers_updated.append(row)

          logger.info(f'Successfully Written Zacks data into database {symbol}')
      except AttributeError as e:
          logger.error(f'Most likely an ETF and therefore not written to database, removed from df_tickers: {symbol}')

  success = sql_close_db(connection, cursor)

  return df_tickers_updated, success


def get_finwiz_stock_data(df_tickers, logger):
  success = False
  for index, row in df_tickers.iterrows():
    ticker = row['Ticker']  

    logger.info(f'Getting finwiz stock data for {ticker}')

    df_company_data = pd.DataFrame()
    url_finviz = "https://finviz.com/quote.ashx?t=%s" % (ticker)
    try:
      page = get_page(url_finviz)

      soup = BeautifulSoup(page.content, 'html.parser')

      table = soup.find_all('table')
      table_rows = table[9].find_all('tr', recursive=False)

      emptyDict = {}

      #Get rows of data.
      for tr in table_rows:
          tds = tr.find_all('td')
          boolKey = True
          keyValueSet = False
          for td in tds:
              if boolKey:
                  key = td.text.strip()
                  boolKey = False                
              else:
                  value = td.text.strip()
                  boolKey = True
                  keyValueSet = True                

              if keyValueSet:
                  emptyDict[key] = value
                  keyValueSet = False

      df_company_data.loc[ticker, 'PE'] = emptyDict['P/E']
      df_company_data.loc[ticker, 'EPS_TTM'] = emptyDict['EPS (ttm)']
      df_company_data.loc[ticker, 'PE_FORWARD'] = emptyDict['Forward P/E']
      df_company_data.loc[ticker, 'EPS_Y1'] = emptyDict['EPS next Y']
      df_company_data.loc[ticker, 'PEG'] = emptyDict['PEG']
      df_company_data.loc[ticker, 'EPS_Y0'] = emptyDict['EPS this Y']
      df_company_data.loc[ticker, 'PRICE_BOOK'] = emptyDict['P/B']
      df_company_data.loc[ticker, 'PRICE_BOOK'] = emptyDict['P/B']
      df_company_data.loc[ticker, 'PRICE_SALES'] = emptyDict['P/S']
      df_company_data.loc[ticker, 'TARGET_PRICE'] = emptyDict['Target Price']
      df_company_data.loc[ticker, 'ROE'] = emptyDict['ROE']
      df_company_data.loc[ticker, '52W_RANGE'] = emptyDict['52W Range']
      df_company_data.loc[ticker, 'QUICK_RATIO'] = emptyDict['Quick Ratio']
      df_company_data.loc[ticker, 'GROSS_MARGIN'] = emptyDict['Gross Margin']
      df_company_data.loc[ticker, 'CURRENT_RATIO'] = emptyDict['Current Ratio']

      # get ticker cid
      cid = sql_get_cid(ticker)
      if(cid):
        #TODO: write records to database
        rename_cols = {"52W_RANGE": "RANGE_52W"}
        add_col_values = {"cid": cid}
        conflict_cols = "cid"

        success = sql_write_df_to_db(df_company_data, "CompanyRatio", rename_cols, add_col_values, conflict_cols)

      logger.info(f'Successfully retrieved finwiz stock data for {ticker}')

    except Exception as e:
      logger.exception(f'Did not return finwiz stock data for {ticker}: {e}')    

  return success

def get_stockrow_stock_data(df_tickers, logger):
  success = False
  for index, row in df_tickers.iterrows():
    ticker = row['Ticker']  
    count = index
    #import pdb; pdb.set_trace()
    logger.info(f'Getting stockrow data for ({index}) {ticker}')

    df = pd.DataFrame()
    df2 = pd.DataFrame()

    page = get_page_selenium('https://stockrow.com/%s' % (ticker))

    soup = BeautifulSoup(page, 'html.parser')

    try:
      table = soup.find_all('table')[0]
      
      table_rows = table.find_all('tr', recursive=True)
      table_rows_header = table.find_all('tr')[0].find_all('th')

      index = 0

      for header in table_rows_header:
        df.insert(index,header.text,[],True)
        index+=1
      #print("did we get any rows?")
      #print(table_rows)
      #Get rows of data.
      for tr in table_rows:

        if(tr.find_all('td')):
          #print(tr.find_all('td')[len(tr.find_all('td'))-1].text.strip())
          row_heading = tr.find_all('td')[len(tr.find_all('td'))-1].text.strip().replace("Created with Highcharts 8.2.2foo","")   
          if(row_heading in ['Revenue','EBT','Net Income','PE Ratio','Earnings/Sh','Total Debt','Cash Flow/Sh','Book Value/Sh']):
            tds = tr.find_all('td', recursive=True)
            if(tds):
              temp_row = []
              for td in tds:
                temp_row.append(td.text.strip().replace("Created with Highcharts 8.2.2foo",""))        

              df.loc[len(df.index)] = temp_row

      df.rename(columns={ df.columns[13]: "YEAR" }, inplace = True)

      # get a list of columns
      cols = list(df)

      # move the column to head of list using index, pop and insert
      cols.insert(0, cols.pop(cols.index('YEAR')))

      # reorder
      df = df.loc[:, cols]

    except IndexError as e:
      logger.exception(f'Did not load table for {ticker} from stockrow')

    #print("df before df2 is populated. Does it contain data?")
    #print(df)

    page = get_page_selenium("https://www.wsj.com/market-data/quotes/%s/financials/annual/income-statement" % (ticker))

    soup = BeautifulSoup(page, 'html.parser')
    tables = soup.find_all('table')
    try:
      table_rows = tables[0].find_all('tr', recursive=True)

      table_rows_header = tables[0].find_all('tr')[0].find_all('th')

      #TODO: Get EBITDA from table
      
      index = 0
      for header in table_rows_header:
        if(index == 0):
          df2.insert(0,"YEAR",[],True)
        else:
          #import pdb; pdb.set_trace()
          if(header.text.strip()):
            df2.insert(index,header.text.strip(),[],True)
          else:
            #No header exists, so put in a NULL value for the header so that we can remove it later
            df2.insert(index,'NULL',[],True)
        index+=1
      
      #drop the last column has it does not contain any data but rather is a graphic of the trend
      df2 = df2.iloc[: , :-1]
      
      #Insert New Row. Format the data to show percentage as float

      for tr in table_rows:

        if(tr.find_all('td')):
          if(tr.find_all('td')[0].text in ['EBITDA']):
            temp_row = []

            td = tr.find_all('td')
            for obs in td:
              if(len(obs.text.strip()) > 0):
                text = obs.text
                temp_row.append(text)        
            try:
              df2.loc[len(df2.index)] = temp_row
            except ValueError as e:
              #Handling when the html of the table is incorrect and results in an additional element in the row
              logger.exception(f'Mismatched df2 row for {ticker}')
              pass
            break
      #print(df2)
      #df2.drop([" "], axis=1) #Hack: Drop any null columns. Better to just remove them upstream
    except IndexError as e:
      logger.exception(f'Did not load table for {ticker} from wsj')

    #Only proceed to format the data and write to database if we have data about the ticker at this point
    if(df.empty == False):      
      #Lets drop all NULL columns from df2
      if('NULL' in df2):
        df2 = df2.drop(['NULL'],axis=1)  

      df = df.append(df2,ignore_index = True)    
      df_transposed = transpose_df(df)
      #import pdb; pdb.set_trace()
      #Clean the table by dropping any rows that do not have Revenue data
      df_transposed = df_transposed[df_transposed['Revenue'].notna()]    
      df_transposed = df_transposed[df_transposed['Revenue'] != '–']
  
      #if(ticker == 'AB'):
      #  import pdb; pdb.set_trace()
      #df_transposed = df.T
      #new_header = df_transposed.iloc[0] #grab the first row for the header
      #df_transposed = df_transposed[1:] #take the data less the header row
      #df_transposed.columns = new_header #set the header row as the df header

      #import pdb; pdb.set_trace()
      df_transposed = df_transposed.rename(columns={
        "Revenue":"SALES",          
        "EBT":"EBIT",               
        "Net Income":"NET_INCOME",        
        "PE Ratio":"PE_RATIO",          
        "Earnings/Sh":"EARNINGS_PER_SHARE",       
        "Cash Flow/Sh":"CASH_FLOW_PER_SHARE",      
        "Book Value/Sh":"BOOK_VALUE_PER_SHARE",     
        "Total Debt":"TOTAL_DEBT",        
        "EBITDA": "EBITDA"                   
        })  

      try:
        df_transposed = dataframe_convert_to_numeric(df_transposed, 'EBITDA', logger)
      except KeyError as e:
        logger.exception(f'EBITDA does not exist for {ticker}')
      #print("df_transposed before numeric conversion")
      #print(df_transposed)

      #format numeric values in dataframe
      #df_transposed = df_transposed.squeeze()
      #import pdb; pdb.set_trace()
      try:
        #import pdb; pdb.set_trace()
        df_transposed = dataframe_convert_to_numeric(df_transposed, 'SALES', logger)
      except KeyError as e:
        logger.error(f'SALES does not exist for {ticker}')

      try:
        df_transposed = dataframe_convert_to_numeric(df_transposed, 'EBIT', logger)
      except KeyError as e:
        logger.error(f'EBIT does not exist for {ticker}')

      try:
        df_transposed = dataframe_convert_to_numeric(df_transposed, 'NET_INCOME', logger)
      except KeyError as e:
        logger.error(f'NET_INCOME does not exist for {ticker}')

      try:
        df_transposed = dataframe_convert_to_numeric(df_transposed, 'PE_RATIO', logger)
      except KeyError as e:
        logger.error(f'PE_RATIO does not exist for {ticker}')

      try:
        df_transposed = dataframe_convert_to_numeric(df_transposed, 'EARNINGS_PER_SHARE', logger)
      except KeyError as e:
        logger.error(f'EARNINGS_PER_SHARE does not exist for {ticker}')

      try:
        df_transposed = dataframe_convert_to_numeric(df_transposed, 'CASH_FLOW_PER_SHARE', logger)
      except KeyError as e:
        logger.error(f'CASH_FLOW_PER_SHARE does not exist for {ticker}')

      try:
        df_transposed = dataframe_convert_to_numeric(df_transposed, 'BOOK_VALUE_PER_SHARE', logger)
      except KeyError as e:
        logger.error(f'BOOK_VALUE_PER_SHARE does not exist for {ticker}')

      try:
        df_transposed = dataframe_convert_to_numeric(df_transposed, 'TOTAL_DEBT', logger)
      except KeyError as e:
        logger.error(f'TOTAL_DEBT does not exist for {ticker}')

      #import pdb; pdb.set_trace()
      todays_date = date.today()
      one_year_ago = dt(todays_date.year - 1, 12, 31)
      two_year_ago = dt(todays_date.year - 2, 12, 31)
      three_year_ago = dt(todays_date.year - 3, 12, 31)
      one_year_future = dt(todays_date.year + 1, 12, 31)
      two_year_future = dt(todays_date.year + 2, 12, 31)

      list_dates = []
      list_dates.append(str(three_year_ago.year))
      list_dates.append(str(two_year_ago.year))
      list_dates.append(str(one_year_ago.year))
      list_dates.append(str(todays_date.year))
      list_dates.append(str(one_year_future.year))
      list_dates.append(str(two_year_future.year))
      #import pdb; pdb.set_trace()

      #Remove any rows that have dates that are too old or to new
      df_transposed = df_transposed.reset_index()
      df_transposed = df_transposed.loc[df_transposed['index'].isin(list_dates)]
      df_transposed = df_transposed.reset_index(drop=True)
      df_transposed = df_transposed.rename_axis(None, axis=1)
      df_transposed = df_transposed.rename(columns={'index':'YEAR'})
      #import pdb; pdb.set_trace()
      #df_transposed = df_transposed.loc[list_dates]

      #st.write(f'Data for ({count}) {ticker}')
      #st.write(df_transposed)

      #print(f'Data for {ticker}')
      #print(df_transposed)
      #TODO: Write the details to DB
      #YEAR
      #SALES                   float64
      #EBIT                    float64
      #NET_INCOME              float64
      #PE_RATIO                float64
      #EARNINGS_PER_SHARE      float64
      #CASH_FLOW_PER_SHARE     float64
      #BOOK_VALUE_PER_SHARE    float64
      #TOTAL_DEBT              float64
      #EBITDA                  float64    

      # get ticker cid
      cid = sql_get_cid(ticker)
      if(cid):
        # write records to database
        rename_cols = {"YEAR": "FORECAST_YEAR"}
        add_col_values = {"cid": cid}
        conflict_cols = "cid, forecast_year"

        success = sql_write_df_to_db(df_transposed, "CompanyForecast", rename_cols, add_col_values, conflict_cols)

  return success

def get_zacks_balance_sheet_shares(df_tickers, logger):
  success = False
  for index, row in df_tickers.iterrows():
    ticker = row['Ticker']  
    logger.info(f'Getting zacks balance sheet for {ticker}')
    df_balance_sheet_annual = pd.DataFrame()
    df_balance_sheet_quarterly = pd.DataFrame()

    #only balance sheet that shows treasury stock line item
    page = get_page('https://www.zacks.com/stock/quote/%s/balance-sheet' % (ticker))

    soup = BeautifulSoup(page.content, 'html.parser')
    try:
      table = soup.find_all('table')

      table_annual = table[4]
      table_quarterly = table[7]

      df_balance_sheet_annual = convert_html_table_to_df(table_annual,False)
      df_balance_sheet_quarterly = convert_html_table_to_df(table_quarterly,False)

      df_balance_sheet_annual = transpose_df(df_balance_sheet_annual)

      df_balance_sheet_annual = df_balance_sheet_annual.rename(columns={"Preferred Stock":"PREFERRED_STOCK"})                        
      df_balance_sheet_annual = df_balance_sheet_annual.rename(columns={"Common Stock (Par)":"COMMON_STOCK_PAR"})                          
      df_balance_sheet_annual = df_balance_sheet_annual.rename(columns={"Capital Surplus":"CAPITAL_SURPLUS"})                             
      df_balance_sheet_annual = df_balance_sheet_annual.rename(columns={"Retained Earnings":"RETAINED_EARNINGS"})                           
      df_balance_sheet_annual = df_balance_sheet_annual.rename(columns={"Other Equity":"OTHER_EQUITY"})                                
      df_balance_sheet_annual = df_balance_sheet_annual.rename(columns={"Treasury Stock":"TREASURY_STOCK"})                              
      df_balance_sheet_annual = df_balance_sheet_annual.rename(columns={"Total Shareholder's Equity":"TOTAL_SHAREHOLDERS_EQUITY"})                  
      df_balance_sheet_annual = df_balance_sheet_annual.rename(columns={"Total Liabilities & Shareholder's Equity":"TOTAL_LIABILITIES_SHAREHOLDERS_EQUITY"})    
      df_balance_sheet_annual = df_balance_sheet_annual.rename(columns={"Total Common Equity":"TOTAL_COMMON_EQUITY"})                         
      df_balance_sheet_annual = df_balance_sheet_annual.rename(columns={"Shares Outstanding":"SHARES_OUTSTANDING"})                          
      df_balance_sheet_annual = df_balance_sheet_annual.rename(columns={"Book Value Per Share":"BOOK_VALUE_PER_SHARE"})  
      df_balance_sheet_annual = dataframe_convert_to_numeric(df_balance_sheet_annual,'PREFERRED_STOCK', logger)
      df_balance_sheet_annual = dataframe_convert_to_numeric(df_balance_sheet_annual,'COMMON_STOCK_PAR', logger)
      df_balance_sheet_annual = dataframe_convert_to_numeric(df_balance_sheet_annual,'CAPITAL_SURPLUS', logger)
      df_balance_sheet_annual = dataframe_convert_to_numeric(df_balance_sheet_annual,'RETAINED_EARNINGS', logger)
      df_balance_sheet_annual = dataframe_convert_to_numeric(df_balance_sheet_annual,'OTHER_EQUITY', logger)
      df_balance_sheet_annual = dataframe_convert_to_numeric(df_balance_sheet_annual,'TREASURY_STOCK', logger)
      df_balance_sheet_annual = dataframe_convert_to_numeric(df_balance_sheet_annual,'TOTAL_SHAREHOLDERS_EQUITY', logger)
      df_balance_sheet_annual = dataframe_convert_to_numeric(df_balance_sheet_annual,'TOTAL_LIABILITIES_SHAREHOLDERS_EQUITY', logger)
      df_balance_sheet_annual = dataframe_convert_to_numeric(df_balance_sheet_annual,'TOTAL_COMMON_EQUITY', logger)
      df_balance_sheet_annual = dataframe_convert_to_numeric(df_balance_sheet_annual,'SHARES_OUTSTANDING', logger)
      df_balance_sheet_annual = dataframe_convert_to_numeric(df_balance_sheet_annual,'BOOK_VALUE_PER_SHARE', logger)
      df_balance_sheet_annual.reset_index(inplace=True)
      df_balance_sheet_annual = df_balance_sheet_annual.rename(columns = {'index':'DATE'})
      df_balance_sheet_annual['DATE'] = pd.to_datetime(df_balance_sheet_annual['DATE'],format='%m/%d/%Y')
      df_balance_sheet_annual = df_balance_sheet_annual.rename_axis(None, axis=1)

      df_balance_sheet_quarterly = transpose_df(df_balance_sheet_quarterly)
      df_balance_sheet_quarterly = df_balance_sheet_quarterly.rename(columns={"Preferred Stock":"PREFERRED_STOCK"})                        
      df_balance_sheet_quarterly = df_balance_sheet_quarterly.rename(columns={"Common Stock (Par)":"COMMON_STOCK_PAR"})                          
      df_balance_sheet_quarterly = df_balance_sheet_quarterly.rename(columns={"Capital Surplus":"CAPITAL_SURPLUS"})                             
      df_balance_sheet_quarterly = df_balance_sheet_quarterly.rename(columns={"Retained Earnings":"RETAINED_EARNINGS"})                           
      df_balance_sheet_quarterly = df_balance_sheet_quarterly.rename(columns={"Other Equity":"OTHER_EQUITY"})                                
      df_balance_sheet_quarterly = df_balance_sheet_quarterly.rename(columns={"Treasury Stock":"TREASURY_STOCK"})                              
      df_balance_sheet_quarterly = df_balance_sheet_quarterly.rename(columns={"Total Shareholder's Equity":"TOTAL_SHAREHOLDERS_EQUITY"})                  
      df_balance_sheet_quarterly = df_balance_sheet_quarterly.rename(columns={"Total Liabilities & Shareholder's Equity":"TOTAL_LIABILITIES_SHAREHOLDERS_EQUITY"})    
      df_balance_sheet_quarterly = df_balance_sheet_quarterly.rename(columns={"Total Common Equity":"TOTAL_COMMON_EQUITY"})                         
      df_balance_sheet_quarterly = df_balance_sheet_quarterly.rename(columns={"Shares Outstanding":"SHARES_OUTSTANDING"})                          
      df_balance_sheet_quarterly = df_balance_sheet_quarterly.rename(columns={"Book Value Per Share":"BOOK_VALUE_PER_SHARE"})  
      df_balance_sheet_quarterly = dataframe_convert_to_numeric(df_balance_sheet_quarterly,'PREFERRED_STOCK', logger)
      df_balance_sheet_quarterly = dataframe_convert_to_numeric(df_balance_sheet_quarterly,'COMMON_STOCK_PAR', logger)
      df_balance_sheet_quarterly = dataframe_convert_to_numeric(df_balance_sheet_quarterly,'CAPITAL_SURPLUS', logger)
      df_balance_sheet_quarterly = dataframe_convert_to_numeric(df_balance_sheet_quarterly,'RETAINED_EARNINGS', logger)
      df_balance_sheet_quarterly = dataframe_convert_to_numeric(df_balance_sheet_quarterly,'OTHER_EQUITY', logger)
      df_balance_sheet_quarterly = dataframe_convert_to_numeric(df_balance_sheet_quarterly,'TREASURY_STOCK', logger)
      df_balance_sheet_quarterly = dataframe_convert_to_numeric(df_balance_sheet_quarterly,'TOTAL_SHAREHOLDERS_EQUITY', logger)
      df_balance_sheet_quarterly = dataframe_convert_to_numeric(df_balance_sheet_quarterly,'TOTAL_LIABILITIES_SHAREHOLDERS_EQUITY', logger)
      df_balance_sheet_quarterly = dataframe_convert_to_numeric(df_balance_sheet_quarterly,'TOTAL_COMMON_EQUITY', logger)
      df_balance_sheet_quarterly = dataframe_convert_to_numeric(df_balance_sheet_quarterly,'SHARES_OUTSTANDING', logger)
      df_balance_sheet_quarterly = dataframe_convert_to_numeric(df_balance_sheet_quarterly,'BOOK_VALUE_PER_SHARE', logger)
      df_balance_sheet_quarterly.reset_index(inplace=True)
      df_balance_sheet_quarterly = df_balance_sheet_quarterly.rename(columns = {'index':'DATE'})
      df_balance_sheet_quarterly['DATE'] = pd.to_datetime(df_balance_sheet_quarterly['DATE'],format='%m/%d/%Y')
      df_balance_sheet_quarterly = df_balance_sheet_quarterly.rename_axis(None, axis=1)

      # get ticker cid
      cid = sql_get_cid(ticker)
      if(cid):
        #TODO: write records to database
        rename_cols = {"DATE": "DT"}
        add_col_values_annually = {"REPORTING_PERIOD": "annual", "cid": cid}
        conflict_cols = "cid, DT, REPORTING_PERIOD"

        success = sql_write_df_to_db(df_balance_sheet_annual, "BalanceSheet", rename_cols, add_col_values_annually, conflict_cols)

        add_col_values_quarterly = {"REPORTING_PERIOD": "quarterly", "cid": cid}
        success = sql_write_df_to_db(df_balance_sheet_quarterly, "BalanceSheet", rename_cols, add_col_values_quarterly, conflict_cols)

      logger.info(f'Successfully retrieved zacks balance sheet data for {ticker}')
      
    except IndexError as e:
      logger.error(f'No balance sheet for {ticker}')
      pass

  return success

def get_zacks_peer_comparison(df_tickers, logger):
  success = False
  for index, row in df_tickers.iterrows():
    ticker = row['Ticker']  

    logger.info(f'Getting zacks peer comparison for {ticker}')

    df_peer_comparison = pd.DataFrame()
    url = "https://www.zacks.com/stock/research/%s/industry-comparison" % (ticker)

    page = get_page(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    table = soup.find_all('table')

    try:
      table_peer_comparison = table[2]
      df_peer_comparison = convert_html_table_to_df(table_peer_comparison,True)
      new = df_peer_comparison["Symbol"].str.split(' ',n=1,expand=True)
      df_peer_comparison["Ticker"] = new[0]
      df_peer_comparison = df_peer_comparison.drop(['Zacks Rank', 'Symbol'], axis=1)
      df_peer_comparison = df_peer_comparison[df_peer_comparison.Ticker != ticker]

      df_peer_comparison = df_peer_comparison.iloc[:4,:]

      df_peer_comparison = df_peer_comparison.rename(columns={df_peer_comparison.columns[0]: 'PEER_COMPANY'})
      df_peer_comparison = df_peer_comparison.rename(columns={df_peer_comparison.columns[1]: 'PEER_TICKER'})

      # get ticker cid
      cid = sql_get_cid(ticker)
      if(cid):
        # write records to database
        rename_cols = {}
        add_col_values = {"cid": cid}
        conflict_cols = "cid, peer_ticker"
        success = sql_write_df_to_db(df_peer_comparison, "CompanyPeerComparison", rename_cols, add_col_values, conflict_cols)
        logger.info(f'Successfully retrieved Zacks Peer Comparison for {ticker}')
    except IndexError as e:
      logger.exception(f'Did not return Zacks Peer Comparison for {ticker}')      
    except AttributeError as e:
      logger.exception(f'Did not return Zacks Peer Comparison for {ticker}')      
    except KeyError as e:
      logger.exception(f'Did not return Zacks Peer Comparison for {ticker}')      

  return success

def get_zacks_earnings_surprises(df_tickers, logger):
  for index, row in df_tickers.iterrows():
    ticker = row['Ticker']  
    logger.info(f'Getting zacks earnings surprises for {ticker}')

    df_earnings_release_date = pd.DataFrame()
    df_earnings_surprises = pd.DataFrame()
    df_sales_surprises = pd.DataFrame()

    url = "https://www.zacks.com/stock/research/%s/earnings-calendar" % (ticker)

    page = get_page(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    new_df_earnings = pd.DataFrame()
    #Get Earnings Release Date

    try:
      table_earnings_release_date = soup.find_all('table')[2]
      df_earnings_release_date = convert_html_table_to_df(table_earnings_release_date,True)
      df_earnings_release_date['Release Date'] = df_earnings_release_date['Report Date'].str[:10]
      df_earnings_release_date = df_earnings_release_date.drop(['Zacks Consensus Estimate', 'Earnings ESP','Report Date'], axis=1)
      df_earnings_release_date['Release Date'] = pd.to_datetime(df_earnings_release_date['Release Date'],format='%m/%d/%Y')
    except (IndexError,AttributeError) as e:
      logger.exception(f'No earnings date for {ticker}. It is probably an ETF')
      pass
    except (ValueError, KeyError) as e:
      logger.exception(f'Earnings Date is NA for {ticker}')

    #Need to extract Earnings and Sales Surprises data from json object in javascript on page
    #scripts = soup.find_all('script')[29]
    try:
      scripts = soup.find_all('script')[25]
      match_pattern = re.compile(r'(?<=\= ).*\}')
      match_string = scripts.text.strip().replace('\n','')
      matches = match_pattern.findall(match_string)
      match_string = matches[0]
      json_object = json.loads(match_string)

      list_earnings_announcements_earnings = json_object['earnings_announcements_earnings_table']
      list_earnings_announcements_sales = json_object['earnings_announcements_sales_table']

      df_earnings_surprises = convert_list_to_df(list_earnings_announcements_earnings)
      df_earnings_surprises = df_earnings_surprises.drop(df_earnings_surprises.iloc[:, 4:7],axis = 1)
      df_earnings_surprises.rename(columns={ df_earnings_surprises.columns[0]: "DATE",df_earnings_surprises.columns[1]: "PERIOD",df_earnings_surprises.columns[2]: "EPS_ESTIMATE",df_earnings_surprises.columns[3]: "EPS_REPORTED" }, inplace = True)
      df_earnings_surprises['DATE'] = pd.to_datetime(df_earnings_surprises['DATE'],format='%m/%d/%y')
      df_earnings_surprises = dataframe_convert_to_numeric(df_earnings_surprises,'EPS_ESTIMATE', logger)
      df_earnings_surprises = dataframe_convert_to_numeric(df_earnings_surprises,'EPS_REPORTED', logger)

      df_sales_surprises = convert_list_to_df(list_earnings_announcements_sales)
      df_sales_surprises = df_sales_surprises.drop(df_sales_surprises.iloc[:, 4:7],axis = 1)
      df_sales_surprises.rename(columns={ df_sales_surprises.columns[0]: "DATE",df_sales_surprises.columns[1]: "PERIOD",df_sales_surprises.columns[2]: "SALES_ESTIMATE",df_sales_surprises.columns[3]: "SALES_REPORTED" }, inplace = True)
      df_sales_surprises['DATE'] = pd.to_datetime(df_sales_surprises['DATE'],format='%m/%d/%y')
      df_sales_surprises = dataframe_convert_to_numeric(df_sales_surprises,'SALES_ESTIMATE', logger)
      df_sales_surprises = dataframe_convert_to_numeric(df_sales_surprises,'SALES_REPORTED', logger)

      new_df_earnings = pd.merge(df_earnings_surprises, df_sales_surprises,  how='left', left_on=['DATE','PERIOD'], right_on = ['DATE','PERIOD'])

      new_df_earnings = new_df_earnings.iloc[:4,:]

      # get ticker cid
      cid = sql_get_cid(ticker)
      if(cid):
        #TODO: write records to database
        rename_cols = {"DATE": "DT", "PERIOD": "REPORTING_PERIOD"}
        add_col_values = {"cid": cid}
        conflict_cols = "cid, DT, REPORTING_PERIOD"

        success = sql_write_df_to_db(new_df_earnings, "EarningsSurprise", rename_cols, add_col_values, conflict_cols)

      logger.info(f'Successfully retrieved Zacks Searnings Surprises for {ticker}')

    except json.decoder.JSONDecodeError as e:
      logger.exception(f'JSON Loading error in Zacks Earnings Surprises for {ticker}')
      pass

    except IndexError as e:
      logger.exception(f'Did not load earnings or sales surprises for {ticker}')

  return success

def get_zacks_product_line_geography(df_tickers, logger):
  for index, row in df_tickers.iterrows():
    ticker = row['Ticker']  

    logger.info(f'Getting zacks product line and geography for {ticker}')

    df_product_line = pd.DataFrame()
    df_geography = pd.DataFrame()
    pd.set_option('display.max_colwidth', None)

    url = "https://www.zacks.com/stock/research/%s/key-company-metrics-details" % (ticker)

    page = get_page(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    table = soup.find_all('table')

    table_product_line_geography = soup.find_all('table')[2]
    table_rows = table_product_line_geography.find_all('tr')
    
    file_dict = {}
    df = pd.DataFrame()

    #Insert New Row. Format the data to show percentage as float
    for tr in table_rows:
      temp_row = []
      table_rows_header = tr.find_all('th')

      if(len(table_rows_header) > 0):
        if(df.shape[0] > 0):
          file_dict[header_text] = copy.copy(df)     
          df = pd.DataFrame()
        index = 0

        for header in table_rows_header:
          df.insert(index,str(header.text).strip(),[],True)
          index+=1
        header_text = table_rows_header[0].text
        table_rows_header = []
      else:
        td = tr.find_all('td')
        for obs in td:      
          if(obs.p):
            text = obs.p.attrs['title']
          else:
            text = str(obs.text).strip()
          temp_row.append(text)        

        if(len(temp_row) == len(df.columns)):
          df.loc[len(df.index)] = temp_row
    #import pdb; pdb.set_trace()
    if(file_dict):
      try:
        df_product_line = file_dict['Revenue - Line of Business Segments']
        # Clean up dataframes
        df_product_line = df_product_line.drop(columns='YR Estimate', axis=1)
        df_product_line = df_product_line.iloc[:, 0:2]
        colname = df_product_line.columns[1]
        df_product_line = dataframe_convert_to_numeric(df_product_line,colname, logger)
        df_product_line = df_product_line.iloc[:4,:]

        df_product_line = df_product_line.rename(columns={df_product_line.columns[0]: 'BUSINESS_SEGMENT'})
        df_product_line = df_product_line.rename(columns={df_product_line.columns[1]: 'REVENUE'})

        logger.info(f'Successfully retrieved Zacks Product Line for {ticker}')

      except KeyError as e:
        logger.exception(f'Did not load Zacks Product Line for {ticker}')
        pass

      try:
        df_geography = file_dict['Revenue - Geographic Segments']
        df_geography = df_geography.drop(columns='YR Estimate', axis=1)
        df_geography = df_geography.iloc[:, 0:2]
        colname = df_geography.columns[1]
        df_geography = dataframe_convert_to_numeric(df_geography,colname, logger)
        df_geography = df_geography.iloc[:4,:]
        df_geography = df_geography.rename(columns={df_geography.columns[0]: 'REGION'})
        df_geography = df_geography.rename(columns={df_geography.columns[1]: 'REVENUE'})

        # get ticker cid
        cid = sql_get_cid(ticker)
        if(cid):
          #TODO: write records to database
          rename_cols = {}
          add_col_values = {"cid": cid}
          conflict_cols = "cid, REGION"

          success = sql_write_df_to_db(df_geography, "CompanyGeography", rename_cols, add_col_values, conflict_cols)

        logger.info(f'Successfully retrieved Zacks Geography for {ticker}')

      except KeyError as e:
        logger.exception(f'Failed to retrieve Zacks Geography for {ticker}')
        pass
  
  return success

def get_api_json_data(url, filename):

    #check if current file has todays system date, and if it does load from current file. Otherwise, continue to call the api
    file_path = "%s/JSON/%s" % (sys.path[0],filename)
    data_list = []

    todays_date = date.today()
    try:
      file_mod_date = time.ctime(os.path.getmtime(file_path))
      file_mod_date = dt.strptime(file_mod_date, '%a %b %d %H:%M:%S %Y')
    except FileNotFoundError as e:
      #Set file mod date to nothing as we do not have a file
      file_mod_date = None

    try:
        #Check if file date is today. If so, continue. Otherwise, throw exception so that we can use the API instead to load the data
        if(file_mod_date.date() == todays_date):
            my_file = open(file_path, "r")        
        else:
            # Throw exception so that we can read the data from api
            raise Exception('Need to read from API') 
    except Exception as error:
        temp_data = []
        temp_data.append(requests.get(url).json())

        # Write response to File
        with open(file_path, 'w') as f:
            for item in temp_data:
                f.write("%s\n" % item)

        # try to open the file in read mode again
        my_file = open(file_path, "r")        

    data = my_file.read()
    
    # replacing end splitting the text 
    # when newline ('\n') is seen.
    liststr = data.split("\n")
    my_file.close()

    data_list = eval(liststr[0])

    return data_list

def get_api_json_data_no_file(url):

    data_list = []

    data_list.append(requests.get(url).json())

    return data_list

def get_zacks_us_companies():
  list_of_files = glob.glob('data/*.csv',) # * means all if need specific format then *.csv
  latest_zacks_file = max(list_of_files, key=os.path.getctime)
  latest_zacks_file = latest_zacks_file.replace("data\\", "")
  temp_excel_file_path = '/data/{}'.format(latest_zacks_file)

  #Get company data from various sources
  df_us_companies = convert_csv_to_dataframe(temp_excel_file_path)

  return df_us_companies

####################
# Output Functions #
####################

def convert_csv_to_dataframe(excel_file_path):

  if(isWindows):
    filepath = os.getcwd()
    excel_file_path = filepath + excel_file_path.replace("/","\\")

  else:
    filepath = os.path.realpath(__file__)
    excel_file_path = filepath[:filepath.rfind('/')] + excel_file_path

  df = pd.read_csv(excel_file_path)

  return df


####################
# Helper Functions #
####################


def combine_df(df_original, df_new):

  return df_original.combine(df_new, take_larger, overwrite=False)  

def append_two_df(df1, df2):
  merged_data = pd.merge(df1, df2, how='outer', on='DATE')
  return merged_data

def take_larger(s1, s2):
  return s2

def combine_df_on_index(df1, df2, index_col):
  df1 = df1.set_index(index_col)
  df2 = df2.set_index(index_col)

  return df2.combine_first(df1).reset_index()

def convert_html_table_to_df(table, contains_th):
  df = pd.DataFrame()

  try:
    table_rows = table.find_all('tr')
    table_rows_header = table.find_all('tr')[0].find_all('th')
  except AttributeError as e:
    return df
  
  index = 0

  for header in table_rows_header:
    df.insert(index,str(header.text).strip(),[],True)
    index+=1

  #Insert New Row. Format the data to show percentage as float
  for tr in table_rows:
    temp_row = []

    if(contains_th):
      tr_th = tr.find('th')
      text = str(tr_th.text).strip()
      temp_row.append(text)        

    td = tr.find_all('td')
    for obs in td:
      
      exclude = False

      if(obs.find_all('div')):
        if 'hidden' in obs.find_all('div')[0].attrs['class']:
          exclude = True

      if not exclude:
        text = str(obs.text).strip()
        temp_row.append(text)        

    if(len(temp_row) == len(df.columns)):
      df.loc[len(df.index)] = temp_row
  
  return df

def convert_list_to_df(list):

  df = pd.DataFrame(list)

  return df

def _util_check_diff_list(li1, li2):
  # Python code to get difference of two lists
  return list(set(li1) - set(li2))

def dataframe_convert_to_numeric(df, column, logger):
  #TODO: Deal with percentages and negative values in brackets
  try:
    contains_mill = False
    if(df[column].str.contains('m',regex=False).sum() > 0):
      contains_mill = True
      df[column] = df[column].str.replace('m','')

    #contains a billion. Because we are reporting in billions, simply remove the "b"
    if(df[column].str.contains('b',regex=False).sum() > 0):
      df[column] = df[column].str.replace('b','')
    df[column] = df[column].str.replace('N/A','')
    df[column] = df[column].str.replace('NA','')
    df[column] = df[column].str.replace('$','', regex=False)
    df[column] = df[column].str.replace('--','')
    df[column] = df[column].str.replace(',','').replace('–','0.00').replace("-",'0.00')
    df[column] = df[column].str.replace('(','-', regex=True)
    df[column] = df[column].str.replace(')','', regex=True)

  except KeyError as e:
    logger.exception(df)
    logger.exception(column)

  df[column] = pd.to_numeric(df[column])
  if(contains_mill):
    df[column] = df[column]/1000000

  return df

def transpose_df(df):
  df = df.T
  new_header = df.iloc[0] #grab the first row for the header
  df = df[1:] #take the data less the header row
  df.columns = new_header #set the header row as the df header

  return df
  
def handle_exceptions_print_result(future, executor_num, process_num, logger):
  exception = future.exception()
  if exception:
    logger.error(f'EXCEPTION of Executor {executor_num} Process {process_num}: {exception}')
    st.write(f'EXCEPTION of Executor {executor_num} Process {process_num}: {exception}')
  else:
    logger.info(f'Status of Executor {executor_num} Process {process_num}: {future.result()}')
    st.write(f'Status of Executor {executor_num} Process {process_num}: {future.result()}')
######################
# Database Functions #
######################

def sql_get_cid(ticker):

  connection, cursor = sql_open_db()
  #connection = psycopg2.connect(host=config.DB_HOST, database=config.DB_NAME, user=config.DB_USER, password=config.DB_PASS)
  #cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

  sqlCmd = """SELECT cid FROM company WHERE symbol='{}'""".format(sql_escape_str(ticker))
  cursor.execute(sqlCmd)
  try:
    cid = cursor.fetchone()[0]
  except TypeError as e:
    cid = None

  success = sql_close_db(connection, cursor)

  return cid

def sql_write_df_to_db(df, db_table, rename_cols, additional_col_values, conflict_cols):

  #connection = psycopg2.connect(host=config.DB_HOST, database=config.DB_NAME, user=config.DB_USER, password=config.DB_PASS)
  #cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
  connection, cursor = sql_open_db()

  # rename cols based on rename_cols
  df = df.rename(columns=rename_cols)

  for index, row in df.iterrows():
    str1, str2, str3 = "", "", ""

    for name, value in row.items():
      str1 += f'{name},'
      str2 += sql_format_str(value)
      str3 += f'{name}=excluded.{name},'

    #add additional col values based on additional_col_values variable
    if(len(additional_col_values) > 0):
      for key in additional_col_values:
        str1 += f'{key},'
        str2 += sql_format_str(additional_col_values[key])

    #Remove last comma from both str1 and str2
    str1 = str1.rstrip(',')
    str2 = str2.rstrip(',')
    str3 = str3.rstrip(',')
    sqlCmd = """INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {};
    """.format(db_table, str1, str2, conflict_cols, str3)

    cursor.execute(sqlCmd)
    connection.commit()

  success = sql_close_db(connection, cursor)
  #cursor.close()
  #connection.close()

  return success

def sql_escape_str(str):
  str = str.replace("'", "''")
  return str

def sql_format_str(value):
  if(pd.isnull(value)):
      return f'\'0.0\',' 
  elif(isinstance(value, dt)):
    #TODO: Need to convert python datetime into postgresql datetime and append it to str2
    # yyyy-mm-dd
    value = value.strftime('%Y-%m-%d')
    return f'\'{value}\','
  elif(isinstance(value, (int, float))):
    if(math.isnan(value)):
      return f'\'0.0\',' 
    else:
      return f'\'{value}\','
  else:
    return f'\'{sql_escape_str(value)}\','

def sql_open_db():
  connection = psycopg2.connect(host=config.DB_HOST, database=config.DB_NAME, user=config.DB_USER, password=config.DB_PASS)
  cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
  return connection, cursor

def sql_close_db(connection, cursor):
  connection.close()
  cursor.close()
  return True

def get_logger():
  logger = logging.getLogger(__name__)
  logger.setLevel(logging.DEBUG)

  formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')

  file_handler_errors = logging.FileHandler('log_error.log', mode='w')
  file_handler_errors.setFormatter(formatter)
  file_handler_errors.setLevel(logging.ERROR)

  file_handler_all = logging.FileHandler('log_debug.log', mode='w')
  file_handler_all.setFormatter(formatter)
  file_handler_all.setLevel(logging.DEBUG)

  stream_handler = logging.StreamHandler()
  stream_handler.setFormatter(formatter)

  logger.addHandler(file_handler_errors)
  logger.addHandler(file_handler_all)

  logger.addHandler(stream_handler)

  return logger