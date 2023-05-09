import streamlit as st
import pandas as pd
import yfinance as yf
import numpy as np
import requests
import tweepy
import config 
import psycopg2, psycopg2.extras
import plotly.graph_objects as go
import glob
import os
import json
import concurrent.futures
from psycopg2 import sql
from datetime import date
from datetime import datetime as dt
from common import set_finwiz_stock_data, set_stockrow_stock_data, set_zacks_balance_sheet_shares
from common import set_zacks_peer_comparison, set_zacks_earnings_surprises, set_zacks_product_line_geography
from common import set_yf_key_stats, get_zacks_us_companies, handle_exceptions_print_result
from common import write_zacks_ticker_data_to_db, get_logger, get_one_pager
from common import set_earningswhispers_earnings_calendar
from common import set_marketscreener_economic_calendar
from common import set_whitehouse_news, set_geopolitical_calendar, get_data
from common import set_yf_price_action

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

st.markdown(f'''
    <style>
        section[data-testid="stSidebar"] .css-ng1t4o {{width: 14rem;}}
        section[data-testid="stSidebar"] .css-1d391kg {{width: 14rem;}}
    </style>
''',unsafe_allow_html=True)

option = st.sidebar.selectbox("Which Option?", ('Download Data','Macro Economic Data','Calendar', 'Single Stock One Pager', 'Bottom Up Ideas'), 2)

st.header(option)

if option == 'Download Data':

    #num_days = st.sidebar.slider('Number of days', 1, 30, 3)
    clicked1 = st.markdown("Download Macroeconomic Data (takes 1 hour)")
    clicked1 = st.button(label="Click to Download Macro Data",key="macro_data")

    clicked2 = st.markdown("Download Stock Data (takes 6 hours)")
    clicked2 = st.button(label="Click to Download Stock Data", key="stock_data")

    clicked3 = st.markdown("Download ALL Data (takes 9 hours)")
    clicked3 = st.button(label="Click to Download ALL Data", key="all_data")

    if(clicked1):
        #Download Macro Data
        logger = get_logger()
        now_start = dt.now()
        start_time = now_start.strftime("%H:%M:%S")    
       # data = yf.download(symbol, start=date_str_start, end=date_str_today)

        st.write(f'Downloading Macro Economic Data...')
        df_tickers_all = get_zacks_us_companies()        
        with concurrent.futures.ProcessPoolExecutor() as executor:
            e1p1 = executor.submit(set_earningswhispers_earnings_calendar, df_tickers_all, logger)
            e1p2 = executor.submit(set_marketscreener_economic_calendar, logger)
            e1p3 = executor.submit(set_whitehouse_news, logger)
            e1p4 = executor.submit(set_geopolitical_calendar, logger)
            e1p5 = executor.submit(set_yf_price_action, df_tickers_all, logger)

        now_finish = dt.now()
        finish_time = now_finish.strftime("%H:%M:%S")
        difference = now_finish - now_start
        seconds_in_day = 24 * 60 * 60
        total_time = divmod(difference.days * seconds_in_day + difference.seconds, 60)

        st.write(start_time)
        st.write(finish_time)
        st.write(total_time)

        logger.info(f"Start Time: {start_time}")
        logger.info(f"Start Time: {finish_time}")
        logger.info(f"Total Time: {total_time}")

        handle_exceptions_print_result(e1p1, 1, 1, logger)
        handle_exceptions_print_result(e1p2, 1, 2, logger)
        handle_exceptions_print_result(e1p3, 1, 3, logger)
        handle_exceptions_print_result(e1p4, 1, 4, logger)
        handle_exceptions_print_result(e1p5, 1, 5, logger)

    if(clicked2):
        logger = get_logger()
        now_start = dt.now()
        start_time = now_start.strftime("%H:%M:%S")    

        st.write(f'Downloading Stock Data...')
        #Download data from zacks and other sources and store it in the database.
        #Use mutithreading to make the download process faster

        df_tickers_all = get_zacks_us_companies()
        df_tickers, success = write_zacks_ticker_data_to_db(df_tickers_all, logger)
        df_tickers1, df_tickers2, df_tickers3, df_tickers4, df_tickers5 = np.array_split(df_tickers, 5)

        if(debug):
            #DEBUG CODE
            #df_tickers1 = df_tickers.loc[df_tickers['Ticker'].isin(['AAPL','AIMC'])]
            # Write the output of all these functions into the database
            #e1p1 = set_zacks_balance_sheet_shares(df_tickers1, logger)
            #e2p1 = set_zacks_earnings_surprises(df_tickers1, logger)
            #e3p1 = set_zacks_product_line_geography(df_tickers1, logger)
            e4p1 = set_finwiz_stock_data(df_tickers, logger)
            #e5p1 = set_stockrow_stock_data(df_tickers1, logger)
            #e6p1 = set_yf_key_stats(df_tickers1, logger) 
            #e7p1 = set_zacks_peer_comparison(df_tickers5, logger)
            import pdb; pdb.set_trace()

        with concurrent.futures.ProcessPoolExecutor() as executor:
            #Executor 1: set_zacks_balance_sheet_shares
            e1p1 = executor.submit(set_zacks_balance_sheet_shares, df_tickers1, logger)
            e1p2 = executor.submit(set_zacks_balance_sheet_shares, df_tickers2, logger)
            e1p3 = executor.submit(set_zacks_balance_sheet_shares, df_tickers3, logger)
            e1p4 = executor.submit(set_zacks_balance_sheet_shares, df_tickers4, logger)
            e1p5 = executor.submit(set_zacks_balance_sheet_shares, df_tickers5, logger)

            #Executor 2: set_zacks_earnings_surprises
            e2p1 = executor.submit(set_zacks_earnings_surprises, df_tickers1, logger)
            e2p2 = executor.submit(set_zacks_earnings_surprises, df_tickers2, logger)
            e2p3 = executor.submit(set_zacks_earnings_surprises, df_tickers3, logger)
            e2p4 = executor.submit(set_zacks_earnings_surprises, df_tickers4, logger)
            e2p5 = executor.submit(set_zacks_earnings_surprises, df_tickers5, logger)

            #Executor 3: set_zacks_product_line_geography
            e3p1 = executor.submit(set_zacks_product_line_geography, df_tickers1, logger)
            e3p2 = executor.submit(set_zacks_product_line_geography, df_tickers2, logger)
            e3p3 = executor.submit(set_zacks_product_line_geography, df_tickers3, logger)
            e3p4 = executor.submit(set_zacks_product_line_geography, df_tickers4, logger)
            e3p5 = executor.submit(set_zacks_product_line_geography, df_tickers5, logger)

            #Executor 5: set_stockrow_stock_data
            e5p1 = executor.submit(set_stockrow_stock_data, df_tickers1, logger)
            e5p2 = executor.submit(set_stockrow_stock_data, df_tickers2, logger)
            e5p3 = executor.submit(set_stockrow_stock_data, df_tickers3, logger)
            e5p4 = executor.submit(set_stockrow_stock_data, df_tickers4, logger)
            e5p5 = executor.submit(set_stockrow_stock_data, df_tickers5, logger)

            #Executor 6: set_yf_key_stats
            e6p1 = executor.submit(set_yf_key_stats, df_tickers1, logger)
            e6p2 = executor.submit(set_yf_key_stats, df_tickers2, logger)
            e6p3 = executor.submit(set_yf_key_stats, df_tickers3, logger)
            e6p4 = executor.submit(set_yf_key_stats, df_tickers4, logger)
            e6p5 = executor.submit(set_yf_key_stats, df_tickers5, logger)

            #Executor 7: set_zacks_peer_comparison
            e7p1 = executor.submit(set_zacks_peer_comparison, df_tickers1, logger)
            e7p2 = executor.submit(set_zacks_peer_comparison, df_tickers2, logger)
            e7p3 = executor.submit(set_zacks_peer_comparison, df_tickers3, logger)
            e7p4 = executor.submit(set_zacks_peer_comparison, df_tickers4, logger)
            e7p5 = executor.submit(set_zacks_peer_comparison, df_tickers5, logger)

        #Finwiz does not handle concurrent connections so need to run it without multithreading
        finwiz_stock_data_status = set_finwiz_stock_data(df_tickers, logger)
        
        now_finish = dt.now()
        finish_time = now_finish.strftime("%H:%M:%S")
        difference = now_finish - now_start
        seconds_in_day = 24 * 60 * 60
        total_time = divmod(difference.days * seconds_in_day + difference.seconds, 60)

        st.write(start_time)
        st.write(finish_time)
        st.write(total_time)

        logger.info(f"Start Time: {start_time}")
        logger.info(f"Start Time: {finish_time}")
        logger.info(f"Total Time: {total_time}")

        handle_exceptions_print_result(e1p1, 1, 1, logger)
        handle_exceptions_print_result(e1p2, 1, 2, logger)
        handle_exceptions_print_result(e1p3, 1, 3, logger)
        handle_exceptions_print_result(e1p4, 1, 4, logger)
        handle_exceptions_print_result(e1p5, 1, 5, logger)

        handle_exceptions_print_result(e2p1, 2, 1, logger)
        handle_exceptions_print_result(e2p2, 2, 2, logger)
        handle_exceptions_print_result(e2p3, 2, 3, logger)
        handle_exceptions_print_result(e2p4, 2, 4, logger)
        handle_exceptions_print_result(e2p5, 2, 5, logger)

        handle_exceptions_print_result(e3p1, 3, 1, logger)
        handle_exceptions_print_result(e3p2, 3, 2, logger)
        handle_exceptions_print_result(e3p3, 3, 3, logger)
        handle_exceptions_print_result(e3p4, 3, 4, logger)
        handle_exceptions_print_result(e3p5, 3, 5, logger)

        handle_exceptions_print_result(e5p1, 5, 1, logger)
        handle_exceptions_print_result(e5p2, 5, 2, logger)
        handle_exceptions_print_result(e5p3, 5, 3, logger)
        handle_exceptions_print_result(e5p4, 5, 4, logger)
        handle_exceptions_print_result(e5p5, 5, 5, logger)

        handle_exceptions_print_result(e6p1, 6, 1, logger)
        handle_exceptions_print_result(e6p2, 6, 2, logger)
        handle_exceptions_print_result(e6p3, 6, 3, logger)
        handle_exceptions_print_result(e6p4, 6, 4, logger)
        handle_exceptions_print_result(e6p5, 6, 5, logger)

        handle_exceptions_print_result(e7p1, 7, 1, logger)
        handle_exceptions_print_result(e7p2, 7, 2, logger)
        handle_exceptions_print_result(e7p3, 7, 3, logger)
        handle_exceptions_print_result(e7p4, 7, 4, logger)
        handle_exceptions_print_result(e7p5, 7, 5, logger)

        st.write(f'Status of Finwiz Stock Data: {finwiz_stock_data_status}')

    if(clicked3):
        st.write(f'You clicked button 3!')

if option == 'Calendar':
    #st.subheader(f'Calendar')
    df1 = get_data(table="macro_earningscalendar")
    st.markdown("Earnings Calendar")
    st.dataframe(df1)

    df2 = get_data(table="macro_economiccalendar")
    st.markdown("Economic Calendar")
    st.dataframe(df2)

    df3 = get_data(table="macro_whitehouseannouncement")
    st.markdown("Whitehouse News")
    st.dataframe(df3)

    df4 = get_data(table="macro_geopoliticalcalendar")
    st.markdown("Geopolitical Calendar")
    st.dataframe(df4)

if option == 'Macro Economic Data':
    st.subheader(f'Macro Economic Data')

if option == 'Single Stock One Pager':
    st.write("Get 1 page quantitative data for a Company")
    symbol = st.sidebar.text_input("Symbol", value='MSFT', max_chars=None, key=None, type='default')
    clicked = st.sidebar.button("Get One Pager")

    if('single_stock_one_pager_clicked' in st.session_state):
        clicked = st.session_state['single_stock_one_pager_clicked']

    if(clicked):
        if('single_stock_one_pager_clicked' not in st.session_state):
            st.session_state['single_stock_one_pager_clicked'] = True

        option_one_pager = st.sidebar.selectbox("Which Dashboard?", ('Quantitative Data', 'Chart', 'Stock Twits'), 0)

        if option_one_pager == 'Quantitative Data':
            #Get all the data for this stock from the database
            try:
                df_company_details, df_zacks_balance_sheet_shares, df_zacks_earnings_surprises, df_zacks_product_line_geography, df_stockrow_stock_data, df_yf_key_stats, df_zacks_peer_comparison, df_finwiz_stock_data = get_one_pager(symbol)
            except UnboundLocalError as e:
                st.markdown("Company Not Found")
            else:
                # Get High Level Company Details
                company_name = df_company_details['company_name'][0]
                sector = df_company_details['sector'][0]
                industry = df_company_details['industry'][0]
                exchange = df_company_details['exchange'][0]
                market_cap = df_company_details['market_cap'][0]
                shares_outstanding = df_company_details['shares_outstanding'][0]

                #get the value from the text input and get data
                st.subheader(f'{company_name} ({symbol})')
                #st.write(option_one_pager)

                st.markdown("Balance Sheet")
                st.dataframe(df_zacks_balance_sheet_shares)

                st.markdown("Earnings Surprises")
                st.dataframe(df_zacks_earnings_surprises)

                st.markdown("Geography")
                st.dataframe(df_zacks_product_line_geography)

                st.markdown("Stockrow Data")
                st.dataframe(df_stockrow_stock_data)

                st.markdown("YF Key Stats")
                st.dataframe(df_yf_key_stats)

                st.markdown("Peer Comparison")
                st.dataframe(df_zacks_peer_comparison)

                st.markdown("Finwiz Ratios")
                st.dataframe(df_finwiz_stock_data)

        if option_one_pager == 'Chart':
            st.subheader(f'Chart For: {symbol}')
            st.image(f'https://finviz.com/chart.ashx?t={symbol}&ty=c&ta=1&p=d&s=l')

        if option_one_pager == 'Stock Twits':

            st.subheader(f'Stock Twit News For: {symbol}')

            r = requests.get(f"https://api.stocktwits.com/api/2/streams/symbol/{symbol}.json")

            data = r.json()

            for message in data['messages']:
                st.image(message['user']['avatar_url'])
                st.write(message['user']['username'])
                st.write(message['created_at'])
                st.write(message['body'])

if option == 'Bottom Up Ideas':
        option_one_pager = st.sidebar.selectbox("Which Dashboard?", ('Volume','Insider Trading', 'Country Exposure', 'Twitter'), 0)
        if option_one_pager == 'Volume':        
            st.subheader(f'Volume')
        if option_one_pager == 'Insider Trading':        
            st.subheader(f'Insider Trading')

        if option_one_pager == 'Country Exposure':
            st.subheader(f'Country Exposure')

        if option_one_pager == 'Twitter':        
            st.subheader(f'Twitter')

            for username in config.TWITTER_USERNAMES:
                st.subheader(username)
                user = api.get_user(screen_name=username)
                tweets = api.user_timeline(screen_name=username)
                st.image(user.profile_image_url)
                for tweet in tweets:
                    if('$' in tweet.text):
                        words = tweet.text.split(' ')
                        for word in words:
                            if word.startswith('$') and word[1:].isalpha():
                                symbol = word[1:]
                                st.write(symbol)
                                st.write(tweet.text)

                                st.image(f'https://finviz.com/chart.ashx?t={symbol}&ty=c&ta=1&p=d&s=l')