import streamlit as st
import pandas as pd
import yfinance as yf
import numpy as np
import requests
import tweepy
import config 
import psycopg2, psycopg2.extras
from psycopg2 import sql
import plotly.graph_objects as go
import glob
import os
import json
import concurrent.futures

from datetime import date
from datetime import datetime as dt
from common import get_page, get_finwiz_stock_data, get_stockrow_stock_data, get_zacks_balance_sheet_shares
from common import get_zacks_peer_comparison, get_zacks_earnings_surprises, get_zacks_product_line_geography
from common import get_yf_key_stats,transpose_df, get_zacks_us_companies, handle_exceptions_print_result
from common import sql_escape_str, sql_open_db, sql_close_db

debug = False

#Dates
todays_date = date.today()
date_str_today = "%s-%s-%s" % (todays_date.year, todays_date.month, todays_date.day)
date_str_start = "2007-01-01"

one_year_ago = dt(todays_date.year - 1, 12, 31)
two_year_ago = dt(todays_date.year - 2, 12, 31)
three_year_ago = dt(todays_date.year - 3, 12, 31)
list_dates = []
list_dates.append(one_year_ago)
list_dates.append(two_year_ago)
list_dates.append(three_year_ago)

#https://www.youtube.com/watch?v=0ESc1bh3eIg&list=WL&index=16&t=731s

auth = tweepy.OAuthHandler(config.TWITTER_CONSUMER_KEY, config.TWITTER_CONSUMER_SECRET)
auth.set_access_token(config.TWITTER_ACCESS_TOKEN, config.TWITTER_ACCESS_TOKEN_SECRET)
api = tweepy.API(auth)

list_of_files = glob.glob('data/*.csv',) # * means all if need specific format then *.csv
latest_zacks_file = max(list_of_files, key=os.path.getctime)
latest_zacks_file = latest_zacks_file.replace("data\\", "")

option = st.sidebar.selectbox("Which Option?", ('Download Data', 'One Pager'), 1)

st.header(option)

if option == 'Download Data':
    #num_days = st.sidebar.slider('Number of days', 1, 30, 3)
    clicked = st.markdown("Takes approximately 9 hours")

    clicked = st.button("Click to Download")
    if(clicked):
        now_start = dt.now()
        start_time = now_start.strftime("%H:%M:%S")    
        connection, cursor = sql_open_db()

        print("Clicked!")
        #Download data from zacks and other sources and store it in the database.
        #Use mutithreading to make the download process faster

        df_tickers = get_zacks_us_companies()
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
                print(symbol)
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
            except AttributeError as e:
                print(f'Most likely an ETF and therefore not written to database: {symbol}')

        success = sql_close_db(connection, cursor)        

        df_tickers1, df_tickers2, df_tickers3, df_tickers4, df_tickers5 = np.array_split(df_tickers, 5)

        #DEBUG CODE
        #df_tickers1 = df_tickers.loc[df_tickers['Ticker'].isin(['AAPL'])]
        # Write the output of all these functions into the database
        #e1p1 = get_zacks_balance_sheet_shares(df_tickers1)
        #e2p1 = get_zacks_earnings_surprises(df_tickers1)
        #e3p1 = get_zacks_product_line_geography(df_tickers1)
        #e4p1 = get_finwiz_stock_data(df_tickers1)
        #e5p1 = get_stockrow_stock_data(df_tickers1)
        #e6p1 = get_yf_key_stats(df_tickers1) 
        #e7p1 = get_zacks_peer_comparison(df_tickers1)
        #import pdb; pdb.set_trace()

        with concurrent.futures.ProcessPoolExecutor() as executor:
            #Executor 1: get_zacks_balance_sheet_shares
            e1p1 = executor.submit(get_zacks_balance_sheet_shares, df_tickers1)
            e1p2 = executor.submit(get_zacks_balance_sheet_shares, df_tickers2)
            e1p3 = executor.submit(get_zacks_balance_sheet_shares, df_tickers3)
            e1p4 = executor.submit(get_zacks_balance_sheet_shares, df_tickers4)
            e1p5 = executor.submit(get_zacks_balance_sheet_shares, df_tickers5)

            #Executor 2: get_zacks_earnings_surprises
            e2p1 = executor.submit(get_zacks_earnings_surprises, df_tickers1)
            e2p2 = executor.submit(get_zacks_earnings_surprises, df_tickers2)
            e2p3 = executor.submit(get_zacks_earnings_surprises, df_tickers3)
            e2p4 = executor.submit(get_zacks_earnings_surprises, df_tickers4)
            e2p5 = executor.submit(get_zacks_earnings_surprises, df_tickers5)

            #Executor 3: get_zacks_product_line_geography
            e3p1 = executor.submit(get_zacks_product_line_geography, df_tickers1)
            e3p2 = executor.submit(get_zacks_product_line_geography, df_tickers2)
            e3p3 = executor.submit(get_zacks_product_line_geography, df_tickers3)
            e3p4 = executor.submit(get_zacks_product_line_geography, df_tickers4)
            e3p5 = executor.submit(get_zacks_product_line_geography, df_tickers5)

            #Executor 4: get_finwiz_stock_data
            e4p1 = executor.submit(get_finwiz_stock_data, df_tickers1)
            e4p2 = executor.submit(get_finwiz_stock_data, df_tickers2)
            e4p3 = executor.submit(get_finwiz_stock_data, df_tickers3)
            e4p4 = executor.submit(get_finwiz_stock_data, df_tickers4)
            e4p5 = executor.submit(get_finwiz_stock_data, df_tickers5)

            #Executor 5: get_stockrow_stock_data
            e5p1 = executor.submit(get_stockrow_stock_data, df_tickers1)
            e5p2 = executor.submit(get_stockrow_stock_data, df_tickers2)
            e5p3 = executor.submit(get_stockrow_stock_data, df_tickers3)
            e5p4 = executor.submit(get_stockrow_stock_data, df_tickers4)
            e5p5 = executor.submit(get_stockrow_stock_data, df_tickers5)

            #Executor 6: get_yf_key_stats
            e6p1 = executor.submit(get_yf_key_stats, df_tickers1)
            e6p2 = executor.submit(get_yf_key_stats, df_tickers2)
            e6p3 = executor.submit(get_yf_key_stats, df_tickers3)
            e6p4 = executor.submit(get_yf_key_stats, df_tickers4)
            e6p5 = executor.submit(get_yf_key_stats, df_tickers5)

            #Executor 7: get_zacks_peer_comparison
            e7p1 = executor.submit(get_zacks_peer_comparison, df_tickers1)
            e7p2 = executor.submit(get_zacks_peer_comparison, df_tickers2)
            e7p3 = executor.submit(get_zacks_peer_comparison, df_tickers3)
            e7p4 = executor.submit(get_zacks_peer_comparison, df_tickers4)
            e7p5 = executor.submit(get_zacks_peer_comparison, df_tickers5)

        now_finish = dt.now()
        finish_time = now_finish.strftime("%H:%M:%S")
        difference = now_finish - now_start
        seconds_in_day = 24 * 60 * 60

        st.write(start_time)
        st.write(finish_time)
        st.write(divmod(difference.days * seconds_in_day + difference.seconds, 60))

        handle_exceptions_print_result(e1p1, 1, 1)
        handle_exceptions_print_result(e1p2, 1, 2)
        handle_exceptions_print_result(e1p3, 1, 3)
        handle_exceptions_print_result(e1p4, 1, 4)
        handle_exceptions_print_result(e1p5, 1, 5)

        handle_exceptions_print_result(e2p1, 2, 1)
        handle_exceptions_print_result(e2p2, 2, 2)
        handle_exceptions_print_result(e2p3, 2, 3)
        handle_exceptions_print_result(e2p4, 2, 4)
        handle_exceptions_print_result(e2p5, 2, 5)

        handle_exceptions_print_result(e3p1, 3, 1)
        handle_exceptions_print_result(e3p2, 3, 2)
        handle_exceptions_print_result(e3p3, 3, 3)
        handle_exceptions_print_result(e3p4, 3, 4)
        handle_exceptions_print_result(e3p5, 3, 5)

        handle_exceptions_print_result(e4p1, 4, 1)
        handle_exceptions_print_result(e4p2, 4, 2)
        handle_exceptions_print_result(e4p3, 4, 3)
        handle_exceptions_print_result(e4p4, 4, 4)
        handle_exceptions_print_result(e4p5, 4, 5)

        handle_exceptions_print_result(e5p1, 5, 1)
        handle_exceptions_print_result(e5p2, 5, 2)
        handle_exceptions_print_result(e5p3, 5, 3)
        handle_exceptions_print_result(e5p4, 5, 4)
        handle_exceptions_print_result(e5p5, 5, 5)

        handle_exceptions_print_result(e6p1, 6, 1)
        handle_exceptions_print_result(e6p2, 6, 2)
        handle_exceptions_print_result(e6p3, 6, 3)
        handle_exceptions_print_result(e6p4, 6, 4)
        handle_exceptions_print_result(e6p5, 6, 5)

        handle_exceptions_print_result(e7p1, 7, 1)
        handle_exceptions_print_result(e7p2, 7, 2)
        handle_exceptions_print_result(e7p3, 7, 3)
        handle_exceptions_print_result(e7p4, 7, 4)
        handle_exceptions_print_result(e7p5, 7, 5)

if option == 'One Pager':
    clicked = st.markdown("Get quantitative data for ticker")

    symbol = st.sidebar.text_input("Symbol", value='MSFT', max_chars=None, key=None, type='default')
    clicked = st.sidebar.button("Get One Pager")
    if(clicked):
        #get the value from the text input and get data
        st.subheader(f'One Pager For: {symbol}')


