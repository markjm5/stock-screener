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
import psycopg2, psycopg2.extras
import config
import logging
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from datetime import date
from datetime import datetime as dt
from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta

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

def set_yf_key_stats(df_tickers, logger):
  success = False
  for index, row in df_tickers.iterrows():
    ticker = row['Ticker']  

    logger.info(f'Getting YF Key Stats for {ticker}')
    statsDict = {}

    df_company_data = pd.DataFrame()
    url = "https://finance.yahoo.com/quote/%s/key-statistics?p=%s" % (ticker, ticker)
    try:
      page = get_page(url)
      
      soup = BeautifulSoup(page.content, 'html.parser')

      tables = soup.find_all('table')
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
      logger.info(f'Successfully Saved YF Key Stats for {ticker}')     

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
          logger.exception(f'Most likely an ETF and therefore not written to database, removed from df_tickers: {symbol}')

  success = sql_close_db(connection, cursor)

  return df_tickers_updated, success


def set_finwiz_stock_data(df_tickers, logger):
  success = False

  # Load finwiz exclusion list
  csv_file_path = '/data/finwiz_exclusion_list.csv'
  df_exclusion_list = convert_csv_to_dataframe(csv_file_path)

  for index, row in df_tickers.iterrows():
    ticker = row['Ticker']  
    
    if(df_exclusion_list['Ticker'].str.contains(ticker).any() == False):   
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

          logger.info(f'Successfully Saved finwiz stock data for {ticker}')

      except Exception as e:
        logger.exception(f'Did not return finwiz stock data for {ticker}: {e}')    

  return success

def set_stockrow_stock_data(df_tickers, logger):
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

    #Only execute if the stockrow page has a table containing data about the ticker
    if(soup.find_all('table')):
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
      except IndexError as e:
        logger.exception(f'Did not load table for {ticker} from stockrow')

    #Only execute the following if we have a dataframe that contains data
    if(len(df) > 0):
      try:
        df.rename(columns={ df.columns[13]: "YEAR" }, inplace = True)

        # get a list of columns
        cols = list(df)

        # move the column to head of list using index, pop and insert
        cols.insert(0, cols.pop(cols.index('YEAR')))

        # reorder
        df = df.loc[:, cols]
      except IndexError as e:
        logger.exception(f'No YEAR column for {ticker} from stockrow')

    #Get WSJ Data
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

      if('EBITDA' in df_transposed.columns):
        df_transposed = dataframe_convert_to_numeric(df_transposed, 'EBITDA', logger)
        
      if('SALES' in df_transposed.columns):
        df_transposed = dataframe_convert_to_numeric(df_transposed, 'SALES', logger)

      if('EBIT' in df_transposed.columns):
        df_transposed = dataframe_convert_to_numeric(df_transposed, 'EBIT', logger)

      if('NET_INCOME' in df_transposed.columns):
        df_transposed = dataframe_convert_to_numeric(df_transposed, 'NET_INCOME', logger)

      if('PE_RATIO' in df_transposed.columns):
        df_transposed = dataframe_convert_to_numeric(df_transposed, 'PE_RATIO', logger)

      if('EARNINGS_PER_SHARE' in df_transposed.columns):
        df_transposed = dataframe_convert_to_numeric(df_transposed, 'EARNINGS_PER_SHARE', logger)

      if('CASH_FLOW_PER_SHARE' in df_transposed.columns):
        df_transposed = dataframe_convert_to_numeric(df_transposed, 'CASH_FLOW_PER_SHARE', logger)

      if('BOOK_VALUE_PER_SHARE' in df_transposed.columns):
        df_transposed = dataframe_convert_to_numeric(df_transposed, 'BOOK_VALUE_PER_SHARE', logger)

      if('TOTAL_DEBT' in df_transposed.columns):
        df_transposed = dataframe_convert_to_numeric(df_transposed, 'TOTAL_DEBT', logger)

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

      # get ticker cid
      cid = sql_get_cid(ticker)
      if(cid):
        # write records to database
        rename_cols = {"YEAR": "FORECAST_YEAR"}
        add_col_values = {"cid": cid}
        conflict_cols = "cid, forecast_year"
        success = sql_write_df_to_db(df_transposed, "CompanyForecast", rename_cols, add_col_values, conflict_cols)
        logger.info(f'Successfully Saved stockrow data for {ticker}')

  return success

def set_zacks_balance_sheet_shares(df_tickers, logger):
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

        logger.info(f'Successfully Saved zacks balance sheet data for {ticker}')
      
    except IndexError as e:
      logger.exception(f'No balance sheet for {ticker}')
      pass

  return success

def set_zacks_peer_comparison(df_tickers, logger):
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
        rename_cols = None
        add_col_values = {"cid": cid}
        conflict_cols = "cid, peer_ticker"
        success = sql_write_df_to_db(df_peer_comparison, "CompanyPeerComparison", rename_cols, add_col_values, conflict_cols)
        logger.info(f'Successfully Saved Zacks Peer Comparison for {ticker}')
    except IndexError as e:
      logger.exception(f'Did not return Zacks Peer Comparison for {ticker}')      
    except AttributeError as e:
      logger.exception(f'Did not return Zacks Peer Comparison for {ticker}')      
    except KeyError as e:
      logger.exception(f'Did not return Zacks Peer Comparison for {ticker}')      

  return success

def set_zacks_earnings_surprises(df_tickers, logger):
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

        logger.info(f'Successfully Saved Zacks Earnings Surprises for {ticker}')

    except json.decoder.JSONDecodeError as e:
      logger.exception(f'JSON Loading error in Zacks Earnings Surprises for {ticker}')
      pass

    except IndexError as e:
      logger.exception(f'Did not load earnings or sales surprises for {ticker}')

  return success

def set_zacks_product_line_geography(df_tickers, logger):
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

          logger.info(f'Successfully Saved Zacks Geography for {ticker}')

      except KeyError as e:
        logger.exception(f'Failed to retrieve Zacks Geography for {ticker}')
        pass
  
  return success

def set_earningswhispers_earnings_calendar(df_us_companies, logger):
  logger.info("Getting data from Earnings Whispers")

  df = pd.DataFrame()

  # Get earnings calendar for the next fortnight
  for x in range(1, 16):
      print("Day %s" % x)
      earnings_whispers_day_df = scrape_earningswhispers_day(x, df_us_companies)
      df = df.append(earnings_whispers_day_df, ignore_index=True)

  df = df.drop_duplicates(subset='Ticker', keep="first")
  df['Market Cap (Mil)'] = pd.to_numeric(df['Market Cap (Mil)'])
  df = df.sort_values(by=['Market Cap (Mil)'], ascending=False)
  df = df[:10].reset_index(drop=True)

  df['Date'] = pd.to_datetime(df['Date'],format='%A, %B %d, %Y')

  #Clear out old data
  sql_delete_all_rows('Macro_EarningsCalendar')

  #Write new data into table
  rename_cols = {'Date':'dt','Time':'dt_time','Ticker':'ticker','Company Name':'company_name','Market Cap (Mil)':'market_cap_mil'}
  add_col_values = None
  conflict_cols = None

  success = sql_write_df_to_db(df, "Macro_EarningsCalendar", rename_cols, add_col_values, conflict_cols)

  logger.info("Successfully scraped data from Earnings Whispers")

  return success

def scrape_earningswhispers_day(day, df_us_companies):
  url = "https://www.earningswhispers.com/calendar?sb=c&d=%s&t=all" % (day,)

  page = get_page_selenium(url)

  #soup = BeautifulSoup(page.content, 'html.parser')
  soup = BeautifulSoup(page, 'html.parser')

  date_str = soup.find('div', attrs={"id":"calbox"})
  date_str = date_str.text.strip().replace('for ','')

  eps_cal_table = soup.find('ul', attrs={"id":"epscalendar"})

  table_rows = eps_cal_table.find_all('li')

  df = pd.DataFrame()
  
  # Add Date, Time, CompanyName, Ticker headers to dataframe
  df.insert(0,"Date",[],True)
  df.insert(1,"Time",[],True)
  df.insert(2,"Ticker",[],True)
  df.insert(3,"Company Name",[],True)
  df.insert(4,"Market Cap (Mil)",[],True)

  skip_first = True

  for tr in table_rows:        
      temp_row = []

      td = tr.find_all('div')

      # Just Extract Date, Time, CompanyName, Ticker, EPS, Revenue, Expected Revenue
      for obs in td:  
          text = str(obs.text).strip()
          temp_row.append(text)    

      #import pdb; pdb.set_trace()
      time_str = temp_row[4]
      company_name_str = temp_row[2]
      ticker_str = temp_row[3]

      if(time_str.find(' ET') != -1):
          # Only if company exists on US stocks list, we add to df
          df_retrieved_company_data = df_us_companies.loc[df_us_companies['Ticker'] == ticker_str].reset_index(drop=True)
          if(df_retrieved_company_data.shape[0] > 0):
              temp_row1 = []
              temp_row1.append(date_str)
              temp_row1.append(time_str)
              temp_row1.append(ticker_str)
              temp_row1.append(company_name_str)

              # Get market cap from US Stocks list
              temp_row1.append(df_retrieved_company_data['Market Cap (mil)'].iloc[0])

              if not skip_first:   
                  df.loc[len(df.index)] = temp_row1

      skip_first = False

  return df

def set_marketscreener_economic_calendar(logger):

  logger.info("Getting Economic Calendar from Market Screener")

  url = "https://www.marketscreener.com/stock-exchange/calendar/economic/"

  page = get_page(url)
  soup = BeautifulSoup(page.content, 'html.parser')
  df = pd.DataFrame()

  tables = soup.find_all('table', recursive=True)

  table = tables[0]

  table_rows = table.find_all('tr')

  table_header = table_rows[0]
  td = table_header.find_all('th')
  index = 0

  for obs in td:        
      text = str(obs.text).strip()

      if(len(text)==0):
          text = "Date"
      df.insert(index,text,[],True)
      index+=1

  index = 0
  skip_first = True
  session = ""

  for tr in table_rows:        
      temp_row = []
      #import pdb; pdb.set_trace()
      td = tr.find_all('td')
      #class="card--shadowed"
      if not skip_first:
          td = tr.find_all('td')
          th = tr.find('th') #The time is stored as a th
          if(th):
              temp_row.append(th.text)        

          if(len(td) == 4):
              session = str(td[0].text).strip()

          for obs in td:  

              text = str(obs.text).strip()
              text = text.replace('\n','').replace('  ','')

              if(text == ''):
                  flag_class = obs.i.attrs['class'][2]
                  #Maybe this is the country field, which means that the country is represented by a flag image
                  if(flag_class == 'flag__us'):
                      text = "US"
                  elif(flag_class == 'flag__uk'): 
                      text = "UK"

                  elif(flag_class == 'flag__eu'): 
                      text = "European Union"

                  elif(flag_class == 'flag__de'): 
                      text = "Germany"

                  elif(flag_class == 'flag__jp'): 
                      text = "Japan"

                  elif(flag_class == 'flag__cn'): 
                      text = "China"
                  else:
                      text = "OTHER"
  
              temp_row.append(text)        


          pos1, pos2  = 1, 2

          if(len(temp_row) == len(df.columns)):
              temp_row = swapPositions(temp_row, pos1-1, pos2-1)
          else:
              temp_row.insert(0,session)
              #print(temp_row)
              #import pdb; pdb.set_trace()

          df.loc[len(df.index)] = temp_row
      else:
          skip_first = False

  #Remove Duplicates (Country, Events)
  df = df.drop_duplicates(subset=['Country', 'Events'])

  #Remove OTHER Countries
  df = df[df.Country != 'OTHER'].reset_index(drop=True)

  # Updated the date columns
  df['Date'] = df['Date'].apply(clean_dates)
  
  #Clear out old data
  sql_delete_all_rows('Macro_EconomicCalendar')

  #Write new data into table
  rename_cols = {'Date':'dt','Time':'dt_time','Country':'country','Events':'economic_event','Previous period':'previous'}
  add_col_values = None
  conflict_cols = None

  success = sql_write_df_to_db(df, "Macro_EconomicCalendar", rename_cols, add_col_values, conflict_cols)

  logger.info("Successfully Scraped Economic Calendar from Market Screener")

  return success

def set_whitehouse_news(logger):
  logger.info("Getting Whitehouse news")

  url = "https://www.whitehouse.gov/briefing-room/statements-releases/"
  page = get_page(url)
  soup = BeautifulSoup(page.content, 'html.parser')

  data = {'dt': [], 'post_title':[], 'post_url':[]}

  df = pd.DataFrame(data)

  articles = soup.find_all('article', recursive=True)

  for article in articles:
    temp_row = []

    # Extract Date, Title and Link and put them into a df
    article_title = article.find('a', attrs={'class':'news-item__title'})
    article_date = article.find('time', attrs={'class':'posted-on'})

    post_title = str(article_title.text).strip().replace('\xa0', ' ')
    post_date = article_date.text
    dt_date = pd.to_datetime(post_date,format='%B %d, %Y')
    post_url = article_title.attrs['href']

    temp_row.append(dt_date)
    temp_row.append(post_title)
    temp_row.append(post_url)

    if(len(temp_row) == len(df.columns)):
      df.loc[len(df.index)] = temp_row

  #Clear out old data
  sql_delete_all_rows('Macro_WhitehouseAnnouncement')

  #Write new data into table
  rename_cols = {'Date':'dt','Time':'dt_time','Country':'country','Events':'economic_event','Previous period':'previous'}
  add_col_values = None
  conflict_cols = None

  success = sql_write_df_to_db(df, "Macro_WhitehouseAnnouncement", rename_cols, add_col_values, conflict_cols)

  logger.info("Successfully Scraped Whitehouse news")

  return success

def set_geopolitical_calendar(logger):
  logger.info("Getting Geopolitical Calendar")

  url = "https://www.controlrisks.com/our-thinking/geopolitical-calendar"
  page = get_page(url)
  soup = BeautifulSoup(page.content, 'html.parser')
  df = pd.DataFrame()

  table = soup.find('table', recursive=True)

  table_rows = table.find_all('tr', recursive=True)

  table_rows_header = table.find_all('tr')[0].find_all('th')
  df = pd.DataFrame()

  index = 0

  for header in table_rows_header:
    df.insert(index,str(header.text).strip(),[],True)
    index+=1

  #Insert New Row. Format the data to show percentage as float
  for tr in table_rows:
    temp_row = []

    td = tr.find_all('td')
    for obs in td:
      text = str(obs.text).strip()
      temp_row.append(text)        

    if(len(temp_row) == len(df.columns)):
      df.loc[len(df.index)] = temp_row

  #Drop the last column because it is empty
  df = df.iloc[: , :-1]

  #Rename columns so that they match the database table
  df.rename(columns={ df.columns[0]: "event_date",df.columns[1]: "event_name",df.columns[2]: "event_location" }, inplace = True)

  #Clear out old data
  sql_delete_all_rows('Macro_GeopoliticalCalendar')

  #Write new data into table
  rename_cols = None
  add_col_values = None
  conflict_cols = None

  success = sql_write_df_to_db(df, "Macro_GeopoliticalCalendar", rename_cols, add_col_values, conflict_cols)

  logger.info("Successfully Scraped Geopolitical Calendar")

  return success

def set_price_action_ta(df_tickers, logger):
  is_success = False

  downloaded_data = download_yf_data_as_csv(df_tickers)
  success_yf_price_action = set_yf_price_action(df_tickers, logger)
  success_ta_patterns = set_ta_pattern_stocks(df_tickers, logger)

  if(downloaded_data & success_yf_price_action & success_ta_patterns):  
    is_success = True

  return is_success

def set_yf_price_action(df_tickers, logger):
  data = {'cid': [], 'last_volume':[], 'vs_avg_vol_10d':[], 'vs_avg_vol_3m':[], 'outlook':[], 'percentage_sold':[], 'last_close':[]}
  df_yf_price_action = pd.DataFrame(data)

  logger.info(f"Downloading price action from Yahoo Finance")

  for index, row in df_tickers.iterrows():
    ticker = row['Ticker'] 
    shares_outstanding = row['Shares Outstanding (mil)'] 
    df = get_ticker_price_summary(ticker, shares_outstanding, logger)
    data = [df_yf_price_action, df]
    df_yf_price_action = pd.concat(data, ignore_index=True)
    logger.info(f"Successfully created csv file containing price action for: {ticker}")

  #Clear out old data
  sql_delete_all_rows('CompanyPriceAction')

  #Write new data into table
  rename_cols = None
  add_col_values = None
  conflict_cols = None

  success = sql_write_df_to_db(df_yf_price_action, "CompanyPriceAction", rename_cols, add_col_values, conflict_cols)

  logger.info(f"Successfully Scraped Price Action")

  return success

def get_ticker_price_summary(ticker, shares_outstanding, logger):
  df = pd.DataFrame()
  data = {'cid': [], 'last_volume':[], 'vs_avg_vol_10d':[], 'vs_avg_vol_3m':[], 'outlook':[], 'percentage_sold':[], 'last_close':[]}
  df_price_action_summary = pd.DataFrame(data)
  
  temp_row = []

  filename = "{}.csv".format(ticker)
  
  logger.info(f"Getting data from {ticker} file")
  try:
    df = pd.read_csv('data/daily_prices/{}'.format(filename))
    df['Date'] = pd.to_datetime(df['Date'],format='%Y-%m-%d')

    df_10d = df.tail(10)
    df_3m = df.tail(30)

    avg_vol_10d = df_10d['Volume'].mean()
    avg_vol_3m = df_3m['Volume'].mean()
    last_volume = df.tail(1)['Volume'].values[0]

    prev_close = df[-2:-1]['Adj Close'].values[0]
    last_close = df[-1:]['Adj Close'].values[0]

    #Create calculated metrics
    if(last_volume > 0 and shares_outstanding > 0):
        percentage = last_volume/shares_outstanding
    else:
        percentage = 0

    if(last_volume > 0 and avg_vol_10d > 0):
        vs_avg_vol_10d = last_volume/avg_vol_10d
    else:
        vs_avg_vol_10d = 0

    if(last_volume > 0 and avg_vol_3m > 0):
        vs_avg_vol_3m = last_volume/avg_vol_3m
    else:
        vs_avg_vol_3m = 0

    if(last_close > prev_close):
        outlook = 'bullish'
    else:
        outlook = 'bearish'

    #Write the following into the database

    cid = sql_get_cid(ticker)
    if(cid):
      logger.info(f"Retrieved {cid} for {ticker}")
      temp_row.append(str(cid))
      temp_row.append(last_volume)
      temp_row.append(vs_avg_vol_10d)
      temp_row.append(vs_avg_vol_3m)
      temp_row.append(outlook)
      temp_row.append(percentage)
      temp_row.append(last_close)

      df_price_action_summary.loc[len(df_price_action_summary.index)] = temp_row
  except IndexError as e:
     logger.exception(f"Indexerror for {ticker}")

  return df_price_action_summary

def set_ta_pattern_stocks(df_tickers, logger):
  #data =  {'symbol': [],'company': [], 'sector': [], 'industry': [] , 'last': []}
  data = {'ticker': [], 'pattern': []}
  df_consolidating = pd.DataFrame(data)
  df_breakout = pd.DataFrame(data)

  for index, row in df_tickers.iterrows():
      filename = "{}.csv".format(row['Ticker'])

      df = pd.read_csv('data/daily_prices/{}'.format(filename))
      symbol = row['Ticker']

      if is_consolidating(df, percentage=2.5):
          df_consolidating.loc[len(df_consolidating.index)] = [symbol, 'consolidating']

      if is_breaking_out(df):
          df_breakout.loc[len(df_breakout.index)] = [symbol, 'breakout']

  data = [df_consolidating, df_breakout]
  df_patterns = pd.concat(data, ignore_index=True)

  #Clear out old data
  sql_delete_all_rows("TA_Patterns")

  #Write new data into table
  rename_cols = None
  add_col_values = None
  conflict_cols = None

  success = sql_write_df_to_db(df_patterns, "TA_Patterns", rename_cols, add_col_values, conflict_cols)

  logger.info("Successfully Scraped TA Patterns")

  return success

def is_consolidating(df, percentage=2):
  try:
    recent_candlesticks = df[-15:]

    max_close = recent_candlesticks['Close'].max()
    min_close = recent_candlesticks['Close'].min()

    threshold = 1 - (percentage / 100)
    if min_close > (max_close * threshold):
        return True        
  except IndexError as e:
     pass

  return False

def is_breaking_out(df, percentage=2.5):
  try:
    last_close = df[-1:]['Close'].values[0]

    if is_consolidating(df[:-1], percentage=percentage):
        recent_closes = df[-16:-1]

        if last_close > recent_closes['Close'].max():
            return True
  except IndexError as e:
     pass

  return False

############
#  GETTERS #
############

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
  list_of_files = glob.glob('data/zacks_custom_screen_*.csv',) # * means all if need specific format then *.csv
  latest_zacks_file = max(list_of_files, key=os.path.getctime)
  latest_zacks_file = latest_zacks_file.replace("data\\", "")
  temp_excel_file_path = '/data/{}'.format(latest_zacks_file)

  #Get company data from various sources
  df_us_companies = convert_csv_to_dataframe(temp_excel_file_path)

  return df_us_companies

def get_one_pager(ticker):

  #Initialize dataframes
  df_zacks_balance_sheet_shares = pd.DataFrame()
  df_zacks_earnings_surprises = pd.DataFrame()
  df_zacks_product_line_geography = pd.DataFrame()
  df_stockrow_stock_data = pd.DataFrame()
  df_yf_key_stats = pd.DataFrame()
  df_zacks_peer_comparison = pd.DataFrame()
  df_finwiz_stock_data = pd.DataFrame()

  # get ticker cid
  cid = sql_get_cid(ticker)

  if(cid):
    #TODO: Query database tables and retrieve all data for the ticker
    df_company_details = get_data(table="company", cid=cid)
    df_zacks_balance_sheet_shares = get_data(table="balancesheet",cid=cid)
    df_zacks_earnings_surprises = get_data(table="earningssurprise",cid=cid)
    df_zacks_product_line_geography = get_data(table="companygeography",cid=cid)
    df_stockrow_stock_data = get_data(table="companyforecast",cid=cid)
    df_yf_key_stats = get_data(table="companymovingaverage",cid=cid)
    df_zacks_peer_comparison = get_data(table="companypeercomparison",cid=cid)
    df_finwiz_stock_data = get_data(table="companyratio",cid=cid)

  return df_company_details, df_zacks_balance_sheet_shares, df_zacks_earnings_surprises, df_zacks_product_line_geography, df_stockrow_stock_data, df_yf_key_stats, df_zacks_peer_comparison, df_finwiz_stock_data

def get_data(table=None, cid=None):
  df = sql_get_records_as_df(table, cid)
  return df

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

def download_yf_data_as_csv(df_tickers):
  todays_date = date.today()
  start_date = todays_date - relativedelta(years=2)
  date_str_today = "%s-%s-%s" % (todays_date.year, todays_date.month, todays_date.day)
  date_str_start = "%s-%s-%s" % (start_date.year, start_date.month, start_date.day)

  for index, row in df_tickers.iterrows():
    ticker = row['Ticker'] 
    data = yf.download(ticker, start=date_str_start, end=date_str_today)
    data.to_csv('data/daily_prices/{}.csv'.format(ticker))

  return True


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
    contains_percentage = False
    if(df[column].str.contains('m',regex=False).sum() > 0):
      contains_mill = True
      df[column] = df[column].str.replace('m','')

    if(df[column].str.contains('%',regex=False).sum() > 0):
      contains_percentage = True
      df[column] = df[column].str.replace('%','')

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
    df[column] = df[column].str.replace('+','', regex=True)
    df[column] = df[column].str.replace('>','', regex=True)

  except KeyError as e:
    logger.exception(df)
    logger.exception(column)

  df[column] = pd.to_numeric(df[column])
  if(contains_mill):
    df[column] = df[column]/1000000

  if(contains_percentage):
    df[column] = df[column]/100

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

# Function to clean the names
def clean_dates(date_name):
    pattern_regex = re.compile(r'^(?:MONDAY|TUESDAY|WEDNESDAY|THURSDAY|FRIDAY|SATURDAY|SUNDAY)')
    day_of_week = re.search(pattern_regex,date_name).group(0)

    pattern_regex = re.compile(r'[0-9][0-9]')
    day_of_month = re.search(pattern_regex,date_name).group(0)

    pattern_regex = re.compile(r'(?:JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER)')
    month_of_year = re.search(pattern_regex,date_name).group(0)

    formatted_date_string = "%s %s %s" % (day_of_week, day_of_month, month_of_year)

    todays_date = date.today()
    todays_date_year = todays_date.year
    formatted_date_string_new = "%s %s" % (formatted_date_string, todays_date_year)
    dt_date = pd.to_datetime(formatted_date_string_new,format='%A %d %B %Y')
    
    # Check if date is in the past. If the date is in the past, change the year to next year
    if(dt_date.to_pydatetime().date() < todays_date):
      todays_date_year += 1
      formatted_date_string_new = "%s %s" % (formatted_date_string, todays_date_year)
      dt_date = pd.to_datetime(formatted_date_string_new,format='%A %d %B %Y')

    #return formatted_date_string
    return dt_date

# Swap function
def swapPositions(list, pos1, pos2):
     
    list[pos1], list[pos2] = list[pos2], list[pos1]
    return list

######################
# Database Functions #
######################

def sql_get_cid(ticker):

  connection, cursor = sql_open_db()

  sqlCmd = """SELECT cid FROM company WHERE symbol='{}'""".format(sql_escape_str(ticker))
  cursor.execute(sqlCmd)
  try:
    cid = cursor.fetchone()[0]
  except TypeError as e:
    cid = None

  success = sql_close_db(connection, cursor)

  return cid

def sql_get_cid(ticker):

  connection, cursor = sql_open_db()

  sqlCmd = """SELECT cid FROM company WHERE symbol='{}'""".format(sql_escape_str(ticker))
  cursor.execute(sqlCmd)
  try:
    cid = cursor.fetchone()[0]
  except TypeError as e:
    cid = None

  success = sql_close_db(connection, cursor)

  return cid


def sql_write_df_to_db(df, db_table, rename_cols, additional_col_values, conflict_cols):

  connection, cursor = sql_open_db()
  if(rename_cols):
    # rename cols based on rename_cols
    df = df.rename(columns=rename_cols)

  for index, row in df.iterrows():
    str1, str2, str3 = "", "", ""

    for name, value in row.items():
      str1 += f'{name},'
      str2 += sql_format_str(value)
      str3 += f'{name}=excluded.{name},'

    #add additional col values based on additional_col_values variable
    if(additional_col_values):
      for key in additional_col_values:
        str1 += f'{key},'
        str2 += sql_format_str(additional_col_values[key])

    #Remove last comma from both str1 and str2
    str1 = str1.rstrip(',')
    str2 = str2.rstrip(',')
    str3 = str3.rstrip(',')
    if(conflict_cols):
      sqlCmd = """INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {};
      """.format(db_table, str1, str2, conflict_cols, str3)
    else:
      sqlCmd = """INSERT INTO {} ({}) VALUES ({});
      """.format(db_table, str1, str2, str3)
       
    cursor.execute(sqlCmd)
    connection.commit()

  success = sql_close_db(connection, cursor)

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

def sql_get_records_as_df(table, cid):
  #df = pd.DataFrame()
  connection, cursor = sql_open_db()
  if(cid):
    sqlCmd = """SELECT * FROM {} WHERE cid={}""".format(table, cid)
  else:
    sqlCmd = """SELECT * FROM {}""".format(table)
     
  cursor.execute(sqlCmd)

  #df = pd.read_sql(sqlCmd,connection)

  colnames = [desc[0] for desc in cursor.description]
  df = pd.DataFrame(cursor.fetchall())
  if(len(df) > 0):
    df.columns = colnames
  success = sql_close_db(connection, cursor)
  return df

def sql_delete_all_rows(table):
  connection, cursor = sql_open_db()
  sqlCmd = """TRUNCATE {} RESTART IDENTITY;""".format(table)
  cursor.execute(sqlCmd)
  connection.commit()  
  success = sql_close_db(connection, cursor)
  return success   

def sql_open_db():
  connection = psycopg2.connect(host=config.DB_HOST, database=config.DB_NAME, user=config.DB_USER, password=config.DB_PASS)
  cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
  return connection, cursor

def sql_close_db(connection, cursor):
  connection.close()
  cursor.close()
  return True

#####################
# Logging Functions #
#####################

def get_logger():

  logs_dir = 'logs/'
  error_logfile = dt.now().strftime('log_error_%Y%m%d_%H%M%S.log')
  debug_logfile = dt.now().strftime('log_debug_%Y%m%d_%H%M%S.log')

  logger = logging.getLogger(__name__)
  logger.setLevel(logging.DEBUG)

  formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')

  file_handler_errors = logging.FileHandler(logs_dir + error_logfile, mode='w')
  file_handler_errors.setFormatter(formatter)
  file_handler_errors.setLevel(logging.ERROR)

  file_handler_all = logging.FileHandler(logs_dir + debug_logfile, mode='w')
  file_handler_all.setFormatter(formatter)
  file_handler_all.setLevel(logging.DEBUG)

  stream_handler = logging.StreamHandler()
  stream_handler.setFormatter(formatter)

  logger.addHandler(file_handler_errors)
  logger.addHandler(file_handler_all)

  logger.addHandler(stream_handler)

  return logger


#### TODO #######
"""
def set_insider_trades_company(df_tickers, logger):

  for index, row in df_tickers.iterrows():
    symbol = row['Ticker']

    url = "http://openinsider.com/search?q=%s" % (symbol,)

    print("Getting Insider Trading Data: %s" % symbol)
    page = get_page_selenium(url)

    soup = BeautifulSoup(page, 'html.parser')
    table = soup.find_all('table')[11]
    df = process_insider_trading_table(table, logger)

    #TODO: Write to database
    import pdb; pdb.set_trace()
    #TODO: make an aggregate line item relating to qty and value, compared to total shares
    #TODO: add to df of consolidate metrics for all symbols                

  return True
"""

def set_todays_insider_trades(logger):

  url = "http://openinsider.com/insider-purchases"
  page = get_page_selenium(url)
  soup = BeautifulSoup(page, 'html.parser')
  table = soup.find_all('table')[11]

  df = convert_html_table_insider_trading_to_df(table, True)

  df.loc[df["percentage_owned"] == "New", "percentage_owned"] = "0"

  df = dataframe_convert_to_numeric(df,'percentage_owned', logger)

  df = df.sort_values(by=['percentage_owned'], ascending=False)

  df['filing_date'] = pd.to_datetime(df['filing_date'],format='%Y-%m-%d')

  #Clear out old data
  sql_delete_all_rows("Macro_InsiderTrading")

  #Write new data into table
  rename_cols = None
  add_col_values = None
  conflict_cols = None

  success = sql_write_df_to_db(df, "Macro_InsiderTrading", rename_cols, add_col_values, conflict_cols)

  logger.info("Successfully Scraped Todays Insider Trades")

  return success


def convert_html_table_insider_trading_to_df(table, contains_th):
  data =  {'filing_date':[],'company_ticker':[],'company_name':[],'insider_name':[],'insider_title':[],'trade_type':[],'trade_price':[],'percentage_owned':[]}

  df = pd.DataFrame(data)
  
  try:
    table_rows = table.find_all('tr')
  except AttributeError as e:
    return df

  first_row = True

  for tr in table_rows:
    temp_row = []

    if(first_row):
      first_row = False
    else:
      td = tr.find_all('td')
      for obs in td:
        text = str(obs.text).strip()
        temp_row.append(text)        

      filing_date = temp_row[1]
      company_ticker = temp_row[3]
      company_name = temp_row[4] # Name
      insider_name = temp_row[5] 
      insider_title = temp_row[6] 
      trade_type = temp_row[7] 
      trade_price = temp_row[8]
      percentage_owned = temp_row[11]

      df.loc[len(df.index)] = [filing_date,company_ticker,company_name,insider_name,insider_title,trade_type,trade_price,percentage_owned]

  return df

  

