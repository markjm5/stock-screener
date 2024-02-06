import streamlit as st
import pandas as pd
import yfinance as yf
import numpy as np
import requests
import tweepy
import config 
import psycopg2, psycopg2.extras
import plotly.graph_objects as go
from matplotlib import pyplot as plt
import seaborn as sns
import glob
import os
import json
import concurrent.futures
from psycopg2 import sql
from datetime import date
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta
from common import set_finwiz_stock_data, set_stockrow_stock_data, set_zacks_balance_sheet_shares
from common import set_zacks_peer_comparison, set_zacks_earnings_surprises, set_zacks_product_line_geography
from common import set_yf_key_stats, get_zacks_us_companies, handle_exceptions_print_result
from common import write_zacks_ticker_data_to_db, get_logger, get_one_pager,atr_to_excel
from common import set_earningswhispers_earnings_calendar, get_atr_prices, get_stlouisfed_data
from common import set_marketscreener_economic_calendar, dataframe_convert_to_numeric
from common import set_whitehouse_news, set_geopolitical_calendar, get_data, sql_get_volume, set_yf_historical_data
from common import set_price_action_ta, set_todays_insider_trades, combine_df_on_index
from common import style_df_for_display, style_df_for_display_date, format_fields_for_dashboard, get_yf_price_action
from common import format_df_for_dashboard_flip, format_df_for_dashboard, format_volume_df, set_country_credit_rating
from common import set_stlouisfed_data, temp_load_excel_data_to_db, set_ism_manufacturing, set_ism_services
from common import display_chart, display_chart_ism, append_two_df, standard_display, display_chart_assets
from common import calculate_etf_performance, calculate_annual_etf_performance, format_bullish_bearish, format_earnings_surprises
from common import get_financialmodelingprep_price_action, set_summary_ratios, get_summary_ratios, set_2y_rates, set_10y_rates, calc_ir_metrics
from common import set_us_treasury_yields, set_financialmodelingprep_dcf, plot_ticker_signals_ema, plot_ticker_signals_vwap, plot_ticker_signals_histogram
from common import import_report_data
import seaborn as sns
from copy import deepcopy

pd.options.mode.chained_assignment = None #Switch off warning

st.set_page_config(
    page_title="Stock Screener App",
    page_icon=":shark:",
    layout="wide",
    initial_sidebar_state="collapsed",
    menu_items={
        'Get Help': 'https://www.extremelycoolapp.com/help',
        'Report a bug': "https://www.extremelycoolapp.com/bug",
        'About': "# This is a header. This is an *extremely* cool app!"
    }
)

#TODO: Set Page Layout: https://www.youtube.com/watch?v=0AhG53TCezg

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
df_tickers_all = get_zacks_us_companies()

#https://www.youtube.com/watch?v=0ESc1bh3eIg&list=WL&index=16&t=731s

auth = tweepy.OAuthHandler(config.TWITTER_CONSUMER_KEY, config.TWITTER_CONSUMER_SECRET)
auth.set_access_token(config.TWITTER_ACCESS_TOKEN, config.TWITTER_ACCESS_TOKEN_SECRET)
api = tweepy.API(auth)

#list_of_files = glob.glob('data/*.csv',) # * means all if need specific format then *.csv
#latest_zacks_file = max(list_of_files, key=os.path.getctime)
#latest_zacks_file = latest_zacks_file.replace("data\\", "")

st.markdown(f'''
    <style>
        section[data-testid="stSidebar"] .css-ng1t4o {{width: 14rem;}}
        section[data-testid="stSidebar"] .css-1d391kg {{width: 14rem;}}
    </style>
''',unsafe_allow_html=True)

option = st.sidebar.selectbox("Which Option?", ('Download Data','Market Data','Macroeconomic Data','Calendar', 'Single Stock One Pager','ATR Calculator', 'Bottom Up Ideas', 'Trading Report'), 2)

st.header(option)

if option == 'Download Data':

    #num_days = st.sidebar.slider('Number of days', 1, 30, 3)
    clicked1 = st.markdown("Download Economic Calendar Data (takes 15 minutes)")
    clicked1 = st.button(label="Click to Download Economic Calendar Data",key="economic_cal_data")

    clicked2 = st.markdown("Download Stock Data (takes 2 hours)")
    clicked2 = st.button(label="Click to Download Stock Data", key="stock_data")

    clicked3 = st.markdown("Download Stock Row Data (takes 6 hours)")
    clicked3 = st.button(label="Click to Download Stock Row Data", key="stock_row_data")

    clicked4 = st.markdown("Download Macroeconomic Data (takes 1 hour)")
    clicked4 = st.button(label="Click to Download Macroeconomic Data", key="macro_data")

    if(clicked1):
        #Download Macro Data
        logger = get_logger()
        now_start = dt.now()
        start_time = now_start.strftime("%H:%M:%S")    

        st.write(f'{start_time} - Downloading Economic Calendars...')
        #df_tickers = get_data(table="company") 
        with concurrent.futures.ProcessPoolExecutor() as executor:
            e1p1 = executor.submit(set_earningswhispers_earnings_calendar, df_tickers_all, logger)
            e1p2 = executor.submit(set_marketscreener_economic_calendar, logger)
            e1p3 = executor.submit(set_whitehouse_news, logger)
            e1p4 = executor.submit(set_geopolitical_calendar, logger)

        now_finish = dt.now()
        finish_time = now_finish.strftime("%H:%M:%S")
        difference = now_finish - now_start
        seconds_in_day = 24 * 60 * 60
        minutes, seconds = divmod(difference.days * seconds_in_day + difference.seconds, 60)
        total_time = '{:02} minutes {:02} seconds'.format(int(minutes), int(seconds))

        data = {'Start Time':[],'End Time':[],'Total Time':[]}
        df_time = pd.DataFrame(data)
        temp_row = [start_time,finish_time,total_time]
        df_time.loc[len(df_time.index)] = temp_row

        sort_cols = []
        drop_cols = []
        rename_cols = {}
        format_cols = {}

        style_t1 = format_df_for_dashboard(df_time, sort_cols, drop_cols, rename_cols, format_cols=format_cols)
        st.write(style_t1)

        logger.info(f"Start Time: {start_time}")
        logger.info(f"Start Time: {finish_time}")
        logger.info(f"Total Time: {total_time}")

        executor_count = 1
        data = {'Executor':[],'Process':[],'Error':[]}
        df_result = pd.DataFrame(data)
        
        for x in range(1,5):
            result = handle_exceptions_print_result(eval('e{0}p{1}'.format(int(executor_count), int(x))),int(executor_count), int(x), logger)
            temp_row = [executor_count,x,result]
            df_result.loc[len(df_result.index)] = temp_row

        rename_cols = {}
        cols_gradient = ['Error']
        cols_drop = []
        disp, df = style_df_for_display(df_result,cols_gradient,rename_cols,cols_drop)   

        st.markdown(disp.to_html(), unsafe_allow_html=True)

    if(clicked2):
        debug = False

        logger = get_logger()
        now_start = dt.now()
        start_time = now_start.strftime("%H:%M:%S")    

        st.write(f'{start_time} - Downloading Stock Data...')
        #Download data from zacks and other sources and store it in the database.
        #Use mutithreading to make the download process faster

        df_tickers, success = write_zacks_ticker_data_to_db(df_tickers_all, logger)
        df_tickers1, df_tickers2, df_tickers3, df_tickers4, df_tickers5 = np.array_split(df_tickers, 5)

        if(debug):
            #DEBUG CODE
            #df_tickers1 = df_tickers.loc[df_tickers['Ticker'].isin(['AAPL','AIMC'])]
            # Write the output of all these functions into the database
            #e1p1 = set_zacks_balance_sheet_shares(df_tickers1, logger)
            #e2p1 = set_zacks_earnings_surprises(df_tickers1, logger)
            e3p1 = set_zacks_product_line_geography(df_tickers1, logger)
            #e4p1 = set_yf_key_stats(df_tickers, logger)
            #e5p1 = set_zacks_peer_comparison(df_tickers4, logger)
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

            #Executor 4: set_yf_key_stats
            e4p1 = executor.submit(set_yf_key_stats, df_tickers1, logger)
            e4p2 = executor.submit(set_yf_key_stats, df_tickers2, logger)
            e4p3 = executor.submit(set_yf_key_stats, df_tickers3, logger)
            e4p4 = executor.submit(set_yf_key_stats, df_tickers4, logger)
            e4p5 = executor.submit(set_yf_key_stats, df_tickers5, logger)

            #Executor 5: set_zacks_peer_comparison
            e5p1 = executor.submit(set_zacks_peer_comparison, df_tickers1, logger)
            e5p2 = executor.submit(set_zacks_peer_comparison, df_tickers2, logger)
            e5p3 = executor.submit(set_zacks_peer_comparison, df_tickers3, logger)
            e5p4 = executor.submit(set_zacks_peer_comparison, df_tickers4, logger)
            e5p5 = executor.submit(set_zacks_peer_comparison, df_tickers5, logger)

            #Executor 8: set_insider_trades_company
            #e8p1 = executor.submit(set_insider_trades_company, df_tickers1, logger)
            #e8p2 = executor.submit(set_insider_trades_company, df_tickers2, logger)
            #e8p3 = executor.submit(set_insider_trades_company, df_tickers3, logger)
            #e8p4 = executor.submit(set_insider_trades_company, df_tickers4, logger)
            #e8p5 = executor.submit(set_insider_trades_company, df_tickers5, logger)

        #Finwiz does not handle concurrent connections so need to run it without multithreading
        finwiz_stock_data_status = set_finwiz_stock_data(df_tickers, logger)
        #tmp_stock_ratios_status = set_summary_ratios(df_tickers, logger)
        with concurrent.futures.ProcessPoolExecutor() as executor:
            #Executor 1: set_zacks_balance_sheet_shares
            e6p1 = executor.submit(set_summary_ratios, df_tickers1, logger)
            e6p2 = executor.submit(set_summary_ratios, df_tickers2, logger)
            e6p3 = executor.submit(set_summary_ratios, df_tickers3, logger)
            e6p4 = executor.submit(set_summary_ratios, df_tickers4, logger)
            e6p5 = executor.submit(set_summary_ratios, df_tickers5, logger)

        now_finish = dt.now()
        finish_time = now_finish.strftime("%H:%M:%S")
        difference = now_finish - now_start
        seconds_in_day = 24 * 60 * 60
        minutes, seconds = divmod(difference.days * seconds_in_day + difference.seconds, 60)
        total_time = '{:02} minutes {:02} seconds'.format(int(minutes), int(seconds))

        data = {'Start Time':[],'End Time':[],'Total Time':[]}
        df_time = pd.DataFrame(data)
        temp_row = [start_time,finish_time,total_time]
        df_time.loc[len(df_time.index)] = temp_row

        sort_cols = []
        drop_cols = []
        rename_cols = {}
        format_cols = {}

        style_t1 = format_df_for_dashboard(df_time, sort_cols, drop_cols, rename_cols, format_cols=format_cols)
        st.write(style_t1)

        logger.info(f"Start Time: {start_time}")
        logger.info(f"Start Time: {finish_time}")
        logger.info(f"Total Time: {total_time}")

        executor_count = 1
        data = {'Executor':[],'Process':[],'Error':[]}
        df_result = pd.DataFrame(data)
        
        for x in range(1,6):
            result = handle_exceptions_print_result(eval('e{0}p{1}'.format(int(executor_count), int(x))),int(executor_count), int(x), logger)
            temp_row = [executor_count,x,result]
            df_result.loc[len(df_result.index)] = temp_row

        rename_cols = {}
        cols_gradient = ['Error']
        cols_drop = []
        disp,df = style_df_for_display(df_result,cols_gradient,rename_cols,cols_drop)   
        st.markdown(disp.to_html(), unsafe_allow_html=True)

        executor_count = 2
        data = {'Executor':[],'Process':[],'Error':[]}
        df_result = pd.DataFrame(data)
        
        for x in range(1,6):
            result = handle_exceptions_print_result(eval('e{0}p{1}'.format(int(executor_count), int(x))),int(executor_count), int(x), logger)
            temp_row = [executor_count,x,result]
            df_result.loc[len(df_result.index)] = temp_row

        rename_cols = {}
        cols_gradient = ['Error']
        cols_drop = []
        disp,df = style_df_for_display(df_result,cols_gradient,rename_cols,cols_drop)   
        st.markdown(disp.to_html(), unsafe_allow_html=True)

        executor_count = 3
        data = {'Executor':[],'Process':[],'Error':[]}
        df_result = pd.DataFrame(data)
        
        for x in range(1,6):
            result = handle_exceptions_print_result(eval('e{0}p{1}'.format(int(executor_count), int(x))),int(executor_count), int(x), logger)
            temp_row = [executor_count,x,result]
            df_result.loc[len(df_result.index)] = temp_row

        rename_cols = {}
        cols_gradient = ['Error']
        cols_drop = []
        disp,df = style_df_for_display(df_result,cols_gradient,rename_cols,cols_drop)   
        st.markdown(disp.to_html(), unsafe_allow_html=True)

        executor_count = 4
        data = {'Executor':[],'Process':[],'Error':[]}
        df_result = pd.DataFrame(data)
        
        for x in range(1,6):
            result = handle_exceptions_print_result(eval('e{0}p{1}'.format(int(executor_count), int(x))),int(executor_count), int(x), logger)
            temp_row = [executor_count,x,result]
            df_result.loc[len(df_result.index)] = temp_row

        rename_cols = {}
        cols_gradient = ['Error']
        cols_drop = []
        disp,df = style_df_for_display(df_result,cols_gradient,rename_cols,cols_drop)   
        st.markdown(disp.to_html(), unsafe_allow_html=True)

        executor_count = 5
        data = {'Executor':[],'Process':[],'Error':[]}
        df_result = pd.DataFrame(data)
        
        for x in range(1,6):
            result = handle_exceptions_print_result(eval('e{0}p{1}'.format(int(executor_count), int(x))),int(executor_count), int(x), logger)
            temp_row = [executor_count,x,result]
            df_result.loc[len(df_result.index)] = temp_row

        rename_cols = {}
        cols_gradient = ['Error']
        cols_drop = []
        disp,df = style_df_for_display(df_result,cols_gradient,rename_cols,cols_drop)   
        st.markdown(disp.to_html(), unsafe_allow_html=True)

        st.write(f'Status of Finwiz Stock Data: {finwiz_stock_data_status}')

        executor_count = 6
        data = {'Executor':[],'Process':[],'Error':[]}
        df_result = pd.DataFrame(data)
        
        for x in range(1,6):
            result = handle_exceptions_print_result(eval('e{0}p{1}'.format(int(executor_count), int(x))),int(executor_count), int(x), logger)
            temp_row = [executor_count,x,result]
            df_result.loc[len(df_result.index)] = temp_row

        rename_cols = {}
        cols_gradient = ['Error']
        cols_drop = []
        disp,df = style_df_for_display(df_result,cols_gradient,rename_cols,cols_drop)   
        st.markdown(disp.to_html(), unsafe_allow_html=True)

    if(clicked3):
        debug = False

        logger = get_logger()
        now_start = dt.now()
        start_time = now_start.strftime("%H:%M:%S")    

        st.write(f'{start_time} - Downloading Stockrow Data...')

        df_tickers, success = write_zacks_ticker_data_to_db(df_tickers_all, logger)
        df_tickers1, df_tickers2, df_tickers3, df_tickers4, df_tickers5 = np.array_split(df_tickers, 5)

        if(debug):
            #DEBUG CODE
            #df_tickers1 = df_tickers.loc[df_tickers['Ticker'].isin(['AAPL','AIMC'])]
            # Write the output of all these functions into the database
            e1p4 = set_stockrow_stock_data(df_tickers4, logger)
            import pdb; pdb.set_trace()

        with concurrent.futures.ProcessPoolExecutor() as executor:            
            #Executor 1: set_stockrow_stock_data
            e1p1 = executor.submit(set_stockrow_stock_data, df_tickers1, logger)
            e1p2 = executor.submit(set_stockrow_stock_data, df_tickers2, logger)
            e1p3 = executor.submit(set_stockrow_stock_data, df_tickers3, logger)
            e1p4 = executor.submit(set_stockrow_stock_data, df_tickers4, logger)
            e1p5 = executor.submit(set_stockrow_stock_data, df_tickers5, logger)

        now_finish = dt.now()
        finish_time = now_finish.strftime("%H:%M:%S")
        difference = now_finish - now_start
        seconds_in_day = 24 * 60 * 60
        minutes, seconds = divmod(difference.days * seconds_in_day + difference.seconds, 60)
        total_time = '{:02} minutes {:02} seconds'.format(int(minutes), int(seconds))

        data = {'Start Time':[],'End Time':[],'Total Time':[]}
        df_time = pd.DataFrame(data)
        temp_row = [start_time,finish_time,total_time]
        df_time.loc[len(df_time.index)] = temp_row

        sort_cols = []
        drop_cols = []
        rename_cols = {}
        format_cols = {}

        style_t1 = format_df_for_dashboard(df_time, sort_cols, drop_cols, rename_cols, format_cols=format_cols)
        st.write(style_t1)

        logger.info(f"Start Time: {start_time}")
        logger.info(f"Start Time: {finish_time}")
        logger.info(f"Total Time: {total_time}")

        executor_count = 1
        data = {'Executor':[],'Process':[],'Error':[]}
        df_result = pd.DataFrame(data)
        
        for x in range(1,6):
            result = handle_exceptions_print_result(eval('e{0}p{1}'.format(int(executor_count), int(x))),int(executor_count), int(x), logger)
            temp_row = [executor_count,x,result]
            df_result.loc[len(df_result.index)] = temp_row

        rename_cols = {}
        cols_gradient = ['Error']
        cols_drop = []
        disp,df = style_df_for_display(df_result,cols_gradient,rename_cols,cols_drop)   
        st.markdown(disp.to_html(), unsafe_allow_html=True)

    if(clicked4):
        logger = get_logger()
        now_start = dt.now()
        start_time = now_start.strftime("%H:%M:%S")    

        st.write(f'{start_time} - Downloading Macroeconomic Data...')
        df_historical_etf_data = get_data(table="macro_yfhistoricaletfdata").reset_index(drop=True)

        # format date
        df_historical_etf_data['series_date'] = pd.to_datetime(df_historical_etf_data['series_date'],format='%Y-%m-%d')

        for column in df_historical_etf_data:
            if(column != 'series_date'):
                df_historical_etf_data[column] = pd.to_numeric(df_historical_etf_data[column])

        df_historical_etf_data.sort_values(by='series_date', inplace = True)
        df_historical_etf_data = df_historical_etf_data.reset_index(drop=True)
        #import pdb; pdb.set_trace()
        # Update all St Louis FED Data
        #success = set_stlouisfed_data(config.STLOUISFED_SERIES, logger)

        # Update ISM Manufacturing
        #success = set_ism_manufacturing(logger)  

        #success = set_yf_historical_data(config.YF_ETF_SERIES, logger)

        # Update ISM Services
        #success = set_ism_services(logger)
        #df_tickers, success = write_zacks_ticker_data_to_db(df_tickers_all, logger)
        #TODO: DF Tickers_all = rename col to "symbol"

        df_tickers = get_data(table="company") 
        with concurrent.futures.ProcessPoolExecutor() as executor:
            e1p1 = executor.submit(set_stlouisfed_data, config.STLOUISFED_SERIES, logger)
            e1p2 = executor.submit(set_ism_manufacturing, logger)
            e1p3 = executor.submit(set_ism_services, logger)
            e1p4 = executor.submit(set_yf_historical_data, config.YF_ETF_SERIES,logger)
            e1p5 = executor.submit(set_10y_rates, logger)
            e1p6 = executor.submit(set_2y_rates, logger)
            e1p7 = executor.submit(set_country_credit_rating,logger)
            e1p8 = executor.submit(set_us_treasury_yields,logger)
            e1p9 = executor.submit(set_price_action_ta, df_tickers, logger)
            e1p10 = executor.submit(set_financialmodelingprep_dcf, df_tickers, logger)

        #import pdb; pdb.set_trace()
        calculate_annual_etf_performance_status = calculate_annual_etf_performance(df_historical_etf_data,logger)        
        calculate_etf_performance_status = calculate_etf_performance(df_historical_etf_data,logger)
        st.write(f'Status of Annual ETF Performance: {calculate_annual_etf_performance_status}')
        st.write(f'Status of ETF Performance: {calculate_etf_performance_status}')

        #TODO: Download ADP report and store in database

        #TODO: Use the following code to load data from other excel files into the database
        #sheet_name = 'DB Services ISM'

        #excel_file_path = '/data/temp_macro_data/03_Leading_Indicators/017_Leading_Indicator_US_ISM_Services.xlsm'
        #rename_cols = {
        #    'DATE':'ism_date',
        #    'Arts, Entertainment & Recreation':'arts_entertainment_recreation',
        #    'Other Services':'other_services',
        #    'Health Care & Social Assistance':'health_care_social_assistance',
        #    'Accommodation & Food Services':'accommodation_food_services',
        #    'Finance & Insurance':'finance_insurance',
        #    'Real Estate, Rental & Leasing':'real_estate_rental_leasing',
        #    'Transportation & Warehousing':'transportation_warehousing',
        #    'Mining':'mining',
        #    'Construction':'construction',
        #    'Wholesale Trade':'wholesale_trade',
        #    'Public Administration':'public_administration',
        #    'Professional, Scientific & Technical Services':'professional_scientific_technical_services',
        #    'Agriculture, Forestry, Fishing & Hunting':'agriculture_forestry_fishing_hunting',
        #    'Information':'information',
        #    'Educational Services':'educational_services',
        #    'Management of Companies & Support Services':'management_of_companies_support_services',
        #    'Retail Trade':'retail_trade',
        #    'Utilities':'utilities',
        #}

        #conflict_cols = "ism_date"
        #database_table = 'macro_us_ism_services_headline'
        #success = temp_load_excel_data_to_db(excel_file_path, sheet_name, database_table, rename_cols=rename_cols, conflict_cols=conflict_cols)

        now_finish = dt.now()
        finish_time = now_finish.strftime("%H:%M:%S")
        difference = now_finish - now_start
        seconds_in_day = 24 * 60 * 60
        minutes, seconds = divmod(difference.days * seconds_in_day + difference.seconds, 60)
        total_time = '{:02} minutes {:02} seconds'.format(int(minutes), int(seconds))

        data = {'Start Time':[],'End Time':[],'Total Time':[]}
        df_time = pd.DataFrame(data)
        temp_row = [start_time,finish_time,total_time]
        df_time.loc[len(df_time.index)] = temp_row

        sort_cols = []
        drop_cols = []
        rename_cols = {}
        format_cols = {}

        style_t1 = format_df_for_dashboard(df_time, sort_cols, drop_cols, rename_cols, format_cols=format_cols)
        st.write(style_t1)

        logger.info(f"Start Time: {start_time}")
        logger.info(f"Start Time: {finish_time}")
        logger.info(f"Total Time: {total_time}")

        executor_count = 1
        data = {'Executor':[],'Process':[],'Error':[]}
        df_result = pd.DataFrame(data)
        
        for x in range(1,11):
            result = handle_exceptions_print_result(eval('e{0}p{1}'.format(int(executor_count), int(x))),int(executor_count), int(x), logger)
            temp_row = [executor_count,x,result]
            df_result.loc[len(df_result.index)] = temp_row

        rename_cols = {}
        cols_gradient = ['Error']
        cols_drop = []
        disp,df = style_df_for_display(df_result,cols_gradient,rename_cols,cols_drop)   
        st.markdown(disp.to_html(), unsafe_allow_html=True)

if option == 'Calendar':
    #st.subheader(f'Calendar')
    col1, col2 = st.columns(2)
    df1 = get_data(table="macro_earningscalendar")
    col1.markdown("Earnings Calendar")

    sort_cols = ['dt']
    drop_cols = ['id' ]
    rename_cols = {'dt': 'Date', 'ticker':'Ticker', 'company_name':'Company Name', 'market_cap_mil':'Market Cap (M)'}
    #number_format_cols = ['market_cap_mil']
    format_cols = {'market_cap_mil': 'number', 'dt': 'date' }

    style_t1 = format_df_for_dashboard(df1, sort_cols, drop_cols, rename_cols, format_cols=format_cols)
    col1.write(style_t1)

    df2 = get_data(table="macro_economiccalendar")
    col2.markdown("Economic Calendar")

    sort_cols = ['dt']
    drop_cols = ['id']
    rename_cols = {'dt': 'Date','dt_time': 'Time', 'country':'Country', 'economic_event':'Economic Event', 'previous':'Previous Data'}
    #number_format_cols = []
    format_cols = {'dt': 'date' }
    #import pdb; pdb.set_trace()
    if(len(df2) > 0):
        style_t2 = format_df_for_dashboard(df2, sort_cols, drop_cols, rename_cols,format_cols=format_cols)
        col2.write(style_t2)
    else:
        col2.write("Could not retrieve economic calendar")

    df3 = get_data(table="macro_whitehouseannouncement")
    st.markdown("Whitehouse News")
    #st.dataframe(df3)

    sort_cols = ['dt']
    drop_cols = ['id' ]
    rename_cols = {'dt': 'Date','post_title': 'Title', 'post_url':'URL'}
    #number_format_cols = []
    format_cols = {'dt': 'date' }

    style_t3 = format_df_for_dashboard(df3, sort_cols, drop_cols, rename_cols, format_cols=format_cols)
    st.write(style_t3)

    df4 = get_data(table="macro_geopoliticalcalendar")
    st.markdown("Geopolitical Calendar")
    #st.dataframe(df4)

    sort_cols = []
    drop_cols = ['id']
    rename_cols = {'event_date': 'Date','event_name': 'Event', 'event_location':'Location'}
    number_format_cols = []

    style_t4 = format_df_for_dashboard(df4, sort_cols, drop_cols, rename_cols, number_format_cols)
    st.write(style_t4)

if option == 'Market Data':
    option_indicator_type = st.sidebar.selectbox("Market Data", ('Market Levels','Asset Class Performance'), 0)
    df_annual_performance = get_data(table="macro_etfannualdata").tail(15).reset_index(drop=True)           
    df_annual_performance['series_date'] = df_annual_performance['series_date'].astype('int64')

    df_etf_performance = get_data(table="macro_etfperformance").reset_index(drop=True)           
    df_etf_performance['last_date'] = pd.to_datetime(df_etf_performance['last_date'],format='%Y-%m-%d')
    for column in df_etf_performance:
        if(column != 'last_date' and column != 'asset'):
            df_etf_performance[column] = pd.to_numeric(df_etf_performance[column])

    if option_indicator_type == 'Market Levels':
        st.subheader(f'Market Levels')
        #option_lagging_indicator_charts = st.sidebar.selectbox("Charts", ('002 - US GDP','005 - US Job Market','006 - PCE','007 - US Inflation','009 - US Industrial Production','011 - US Durable Goods', '011 - US Retail Sales'), 0)

        etf_subset_amer = [
            '_dji',
            '_gspc',
            '_ixic',
            '_nya',
            '_gsptse',
            '_mxx',		
        ]

        etf_subset_emea = [
            '_stoxx50e',
            '_ftse',
            '_gdaxi',
            '_fchi',
            '_ibex',
        ]

        etf_subset_apac = [
            '_n225',
            '_hsi',
            '000300_ss',
            '_axjo',
            '0P0001gy56_f',
            '_bsesn',
            '_nsei',        
        ]


        format_cols = {
            'Last Date': lambda t: t.strftime("%m-%d-%Y"),
            'Last': '{:,.2f}'.format, 
            'YTD': '{:,.2%}'.format,
            'Last 5 Days': '{:,.2%}'.format,
            'Last Month': '{:,.2%}'.format,
            'Last 3 Months': '{:,.2%}'.format,
            'Last 5 Years': '{:,.2%}'.format,
        }

        cols_gradient = ['YTD', 'Last 5 Days','Last Month','Last 3 Months','Last 5 Years']
        rename_cols = {'asset': '',
            'last_value': 'Last',
            'last_date': 'Last Date',
            'ytd_pct': 'YTD',
            'last_5_days_pct': 'Last 5 Days',
            'last_month_pct': 'Last Month',
            'last_3_months_pct': 'Last 3 Months',
            'last_5_years_pct': 'Last 5 Years',
        }

        cols_drop = ['ytd_value', 'last_5_days_value', 'last_month_value', 'last_3_months_value', 'last_5_years_value']            

        rename_cols['asset'] = 'Americas'
        df_display_table_amer = df_etf_performance.copy()
        df_display_table = df_display_table_amer[df_display_table_amer['asset'].isin(etf_subset_amer)]
        df_display_table['asset'] = df_display_table['asset'].map(config.RENAME_ETF)
        disp, df_display_table = style_df_for_display(df_display_table,cols_gradient,rename_cols,cols_drop,cols_format=format_cols,format_rows=False)
        st.markdown(disp.to_html(), unsafe_allow_html=True)           

        st.markdown("""---""")
        rename_cols['asset'] = 'Europe, Middle East & Africa'
        df_display_table_emea = df_etf_performance.copy()
        df_display_table = df_display_table_emea[df_display_table_emea['asset'].isin(etf_subset_emea)]
        df_display_table['asset'] = df_display_table['asset'].map(config.RENAME_ETF)
        disp, df_display_table = style_df_for_display(df_display_table,cols_gradient,rename_cols,cols_drop,cols_format=format_cols,format_rows=False)
        st.markdown(disp.to_html(), unsafe_allow_html=True)           

        st.markdown("""---""")
        rename_cols['asset'] = 'Asia Pacific'
        df_display_table_apac = df_etf_performance.copy()
        df_display_table = df_display_table_apac[df_display_table_apac['asset'].isin(etf_subset_apac)]
        df_display_table['asset'] = df_display_table['asset'].map(config.RENAME_ETF)
        disp, df_display_table = style_df_for_display(df_display_table,cols_gradient,rename_cols,cols_drop,cols_format=format_cols,format_rows=False)
        st.markdown(disp.to_html(), unsafe_allow_html=True)           

    if option_indicator_type == 'Asset Class Performance':
        st.subheader(f'Asset Class Performance')
        option_market_indicator_charts = st.sidebar.selectbox("Charts", ('Economic Cycle','Sectors','ETF Performance'), 0)
        if option_market_indicator_charts == 'Economic Cycle':    
            tab1, tab2 = st.tabs(["📈 Annual Returns (Last 15 Years)", "📈 Current Calendar Year"])

            #TAB 1
            tab1.subheader("Annual Returns (Last 15 Years)")

            df_annual_performance_T = df_annual_performance.T
            df_annual_performance_T.columns = df_annual_performance_T.iloc[0]

            #remove first row from DataFrame
            df_annual_performance_T = df_annual_performance_T[1:]
            
            format_cols = {}
            cols_gradient = []

            for column in df_annual_performance_T.columns:
                format_cols[column] = '{:,.2%}'.format
                cols_gradient.append(column)

            rename_cols = {'index': 'Asset Class'}
            cols_drop = ['level_0']

            df_annual_performance_T = df_annual_performance_T.reset_index()

            etf_subset = [
                'spy',
                'eem',
                'vnq',
                'mdy',
                'sly',
                'efa',
                'tip',
                'agg',
                'djp',
                'bil'
            ]
            df_annual_performance_T = df_annual_performance_T[df_annual_performance_T['index'].isin(etf_subset)].reset_index()
            df_annual_performance_T['index'] = df_annual_performance_T['index'].map(config.RENAME_ETF)
            disp, df_sectors_annual_performance = style_df_for_display(df_annual_performance_T,cols_gradient,rename_cols,cols_drop,cols_format=format_cols,format_rows=False)
            tab1.markdown(disp.to_html(), unsafe_allow_html=True)           
            tab1.subheader(f'Phase 1:')
            tab1.markdown('Best Performers: Equities, Credit Market')
            tab1.markdown('Best Sectors: Cyclicals/Financials/Consumer Discretionary/Industrials')
            tab1.markdown('Worst Sectors: Consumer Staples/Health Care/Energy/Telecom/Utilities')
            tab1.subheader(f'Phase 2:')
            tab1.markdown('Best Performers: Equities')
            tab1.markdown('Worst Performers: Government Bonds')					
            tab1.markdown('Best Sectors: Technology/Industrials')					
            tab1.markdown('Worst Sectors: Utilities/Materials')					
                                    
            tab1.subheader(f'Phase 3:') 	
            tab1.markdown('No real underperformer/outperformer between bonds and stocks')					
            tab1.markdown('Best Sectors: Consumer Staples/Utilities/Energy/Health Care/Materials')					
            tab1.markdown('Worst Sectors: Consumer Discretionary/Technology')					
                                    
            tab1.subheader(f'Phase 4:') 	
            tab1.markdown('Best Performers: Government Bonds')					
            tab1.markdown('You will have to reduce exposure')					
            tab1.markdown('Best Sectors: Consumer Staples/Health Care/Telecom/Utilities')					
            tab1.markdown('Worst Sectors: Financials/Materials/Technology/Industrials')

            #TAB 2
            tab2.subheader("Current Calendar Year")
            col1, col2 = tab2.columns(2)

            # Get last column dynamically
            last_col = df_annual_performance_T.columns.to_list()[len(df_annual_performance_T.columns.to_list())-1]
            df_annual_performance_current_year = df_annual_performance_T[['index', last_col]]
            df_annual_performance_current_year = df_annual_performance_current_year.sort_values(by=[last_col]).reset_index()

            # Display Chart
            #RENAME column to DATE
            #df_annual_performance_current_year = df_annual_performance_current_year.rename(columns={last_col: "DATE"})
            x_axis = 'index'
            y_axis = last_col
            chart_settings = {
                "type": "bar",
                "title": "Annual Returns - %s" % last_col, 
                "xlabel": "Asset Classes", 
                "ylabel": "Percentage Return", 
                "ypercentage": True,
            }

            display_chart_assets(chart_settings, df_annual_performance_current_year, x_axis, y_axis, col1)


        if option_market_indicator_charts == 'Sectors':    
            tab1, tab2 = st.tabs(["📈 Annual Returns (Last 15 Years)", "📈 Charts"])

            df_annual_performance_T = df_annual_performance.T
            df_annual_performance_T.columns = df_annual_performance_T.iloc[0]

            #remove first row from DataFrame
            df_annual_performance_T = df_annual_performance_T[1:]
            
            format_cols = {}
            cols_gradient = []

            for column in df_annual_performance_T.columns:
                format_cols[column] = '{:,.2%}'.format
                cols_gradient.append(column)

            rename_cols = {'index': 'Asset Class'}
            cols_drop = ['level_0']

            df_annual_performance_T = df_annual_performance_T.reset_index()

            etf_subset = [
                'xlp',
                'xly',
                'xle',
                'xlf',
                'xlv',
                'xli',
                'xlk',
                'xlb',
                'xlre',
                'xlc',
                'xlu',
                'spy',
                ]

            df_annual_performance_T = df_annual_performance_T[df_annual_performance_T['index'].isin(etf_subset)].reset_index()
            df_annual_performance_T['index'] = df_annual_performance_T['index'].map(config.RENAME_ETF)
            disp, df_sectors_annual_performance = style_df_for_display(df_annual_performance_T,cols_gradient,rename_cols,cols_drop,cols_format=format_cols,format_rows=False)
            tab1.markdown(disp.to_html(), unsafe_allow_html=True)           

            #TAB 2
            col1, col2 = tab2.columns(2)
            #TODO: Retrieve the pre calculated values from the database and display

            last_col = df_annual_performance_T.columns.to_list()[len(df_annual_performance_T.columns.to_list())-1]
            df_annual_performance_current_year = df_annual_performance_T[['index', last_col]]
            df_annual_performance_current_year = df_annual_performance_current_year.sort_values(by=[last_col]).reset_index()

            #TODO: Display Chart
            #RENAME column to DATE
            #df_annual_performance_current_year = df_annual_performance_current_year.rename(columns={last_col: "DATE"})
            x_axis = 'index'
            y_axis = last_col
            chart_settings = {
                "type": "bar",
                "title": "Annual Returns - %s" % last_col, 
                "xlabel": "Asset Classes", 
                "ylabel": "Percentage Return", 
                "ypercentage": True,
            }

            display_chart_assets(chart_settings, df_annual_performance_current_year, x_axis, y_axis, col1)

            # YTD		
            x_axis = 'asset'
            y_axis = 'ytd_pct'
            chart_settings = {
                "type": "bar",
                "title": "Annual Returns - %s" % 'YTD', 
                "xlabel": "Asset Classes", 
                "ylabel": "Percentage Return", 
                "ypercentage": True,
            }
            df = df_etf_performance[df_etf_performance['asset'].isin(etf_subset)].sort_values(by=[y_axis])
            df['asset'] = df['asset'].map(config.RENAME_ETF)
            display_chart_assets(chart_settings, df, x_axis, y_axis, col2)
                         	
            # Last 5 days		
            x_axis = 'asset'
            y_axis = 'last_5_days_pct'
            chart_settings = {
                "type": "bar",
                "title": "Annual Returns - %s" % 'Last 5 Days', 
                "xlabel": "Asset Classes", 
                "ylabel": "Percentage Return", 
                "ypercentage": True,
            }
            df = df_etf_performance[df_etf_performance['asset'].isin(etf_subset)].sort_values(by=[y_axis])
            df['asset'] = df['asset'].map(config.RENAME_ETF)
            display_chart_assets(chart_settings, df, x_axis, y_axis, col1)

            # Last Month		
            x_axis = 'asset'
            y_axis = 'last_month_pct'
            chart_settings = {
                "type": "bar",
                "title": "Annual Returns - %s" % 'Last Month', 
                "xlabel": "Asset Classes", 
                "ylabel": "Percentage Return", 
                "ypercentage": True,
            }
            df = df_etf_performance[df_etf_performance['asset'].isin(etf_subset)].sort_values(by=[y_axis])
            df['asset'] = df['asset'].map(config.RENAME_ETF)
            display_chart_assets(chart_settings, df, x_axis, y_axis, col2)

            # Last 3months		
            x_axis = 'asset'
            y_axis = 'last_3_months_pct'
            chart_settings = {
                "type": "bar",
                "title": "Annual Returns - %s" % 'Last 3 Months', 
                "xlabel": "Asset Classes", 
                "ylabel": "Percentage Return", 
                "ypercentage": True,
            }
            df = df_etf_performance[df_etf_performance['asset'].isin(etf_subset)].sort_values(by=[y_axis])
            df['asset'] = df['asset'].map(config.RENAME_ETF)
            display_chart_assets(chart_settings, df, x_axis, y_axis, col1)

            # Last 5 years	
            x_axis = 'asset'
            y_axis = 'last_5_years_pct'
            chart_settings = {
                "type": "bar",
                "title": "Annual Returns - %s" % 'Last 5 Years', 
                "xlabel": "Asset Classes", 
                "ylabel": "Percentage Return", 
                "ypercentage": True,
            }
            df = df_etf_performance[df_etf_performance['asset'].isin(etf_subset)].sort_values(by=[y_axis])
            df['asset'] = df['asset'].map(config.RENAME_ETF)
            display_chart_assets(chart_settings, df, x_axis, y_axis, col2)

        if option_market_indicator_charts == 'ETF Performance':
            tab1, tab2 = st.tabs(["📈 Table", "📈 Charts"])
            col1, col2 = tab2.columns(2)

            etf_subset = {
                'rxi',
                'xlp',
                'xly',
                'xle',
                'xlf',
                'xlv',
                'xli',
                'xlk',
                'xlb',
                'xlre',
                'xlc',
                'xlu',
                'spy',
                'uso',
                'qqq',
                'iwm',
                'ibb',
                'eem',
                'hyg',
                'vnq',
                'mdy',
                'sly',
                'efa',
                'tip',
                'agg',
                'djp',
                'bil',
                'gc_f',
                'dx_y_nyb',
            }

            #TAB1
            format_cols = {
                'Last Date': lambda t: t.strftime("%m-%d-%Y"),
                'Last': '{:,.2f}'.format, 
                'YTD': '{:,.2%}'.format,
                'Last 5 Days': '{:,.2%}'.format,
                'Last Month': '{:,.2%}'.format,
                'Last 3 Months': '{:,.2%}'.format,
                'Last 5 Years': '{:,.2%}'.format,
            }

            cols_gradient = ['YTD', 'Last 5 Days','Last Month','Last 3 Months','Last 5 Years']
            rename_cols = {'asset': 'Asset Class',
                'last_value': 'Last',
                'last_date': 'Last Date',
                'ytd_pct': 'YTD',
                'last_5_days_pct': 'Last 5 Days',
                'last_month_pct': 'Last Month',
                'last_3_months_pct': 'Last 3 Months',
                'last_5_years_pct': 'Last 5 Years',
            }

            cols_drop = ['ytd_value', 'last_5_days_value', 'last_month_value', 'last_3_months_value', 'last_5_years_value']            
            df_display_table = df_etf_performance.copy()
            df_display_table = df_display_table[df_display_table['asset'].isin(etf_subset)]
            df_display_table['asset'] = df_display_table['asset'].map(config.RENAME_ETF)
            disp, df_display_table = style_df_for_display(df_display_table,cols_gradient,rename_cols,cols_drop,cols_format=format_cols,format_rows=False)
            tab1.markdown(disp.to_html(), unsafe_allow_html=True)           

            #TAB2
            # YTD		
            x_axis = 'asset'
            y_axis = 'ytd_pct'
            chart_settings = {
                "type": "bar",
                "title": "Annual Returns - %s" % 'YTD', 
                "xlabel": "Asset Classes", 
                "ylabel": "Percentage Return", 
                "ypercentage": True,
            }
            df = df_etf_performance[df_etf_performance['asset'].isin(etf_subset)].sort_values(by=[y_axis])
            df['asset'] = df['asset'].map(config.RENAME_ETF)
            display_chart_assets(chart_settings, df, x_axis, y_axis, col1)
                         	
            # Last 5 days		
            x_axis = 'asset'
            y_axis = 'last_5_days_pct'
            chart_settings = {
                "type": "bar",
                "title": "Annual Returns - %s" % 'Last 5 Days', 
                "xlabel": "Asset Classes", 
                "ylabel": "Percentage Return", 
                "ypercentage": True,
            }
            df = df_etf_performance[df_etf_performance['asset'].isin(etf_subset)].sort_values(by=[y_axis])
            df['asset'] = df['asset'].map(config.RENAME_ETF)
            display_chart_assets(chart_settings, df, x_axis, y_axis, col2)

            # Last Month		
            x_axis = 'asset'
            y_axis = 'last_month_pct'
            chart_settings = {
                "type": "bar",
                "title": "Annual Returns - %s" % 'Last Month', 
                "xlabel": "Asset Classes", 
                "ylabel": "Percentage Return", 
                "ypercentage": True,
            }
            df = df_etf_performance[df_etf_performance['asset'].isin(etf_subset)].sort_values(by=[y_axis])
            df['asset'] = df['asset'].map(config.RENAME_ETF)
            display_chart_assets(chart_settings, df, x_axis, y_axis, col1)

            # Last 3months		
            x_axis = 'asset'
            y_axis = 'last_3_months_pct'
            chart_settings = {
                "type": "bar",
                "title": "Annual Returns - %s" % 'Last 3 Months', 
                "xlabel": "Asset Classes", 
                "ylabel": "Percentage Return", 
                "ypercentage": True,
            }
            df = df_etf_performance[df_etf_performance['asset'].isin(etf_subset)].sort_values(by=[y_axis])
            df['asset'] = df['asset'].map(config.RENAME_ETF)
            display_chart_assets(chart_settings, df, x_axis, y_axis, col2)

            # Last 5 years	
            x_axis = 'asset'
            y_axis = 'last_5_years_pct'
            chart_settings = {
                "type": "bar",
                "title": "Annual Returns - %s" % 'Last 5 Years', 
                "xlabel": "Asset Classes", 
                "ylabel": "Percentage Return", 
                "ypercentage": True,
            }
            df = df_etf_performance[df_etf_performance['asset'].isin(etf_subset)].sort_values(by=[y_axis])
            df['asset'] = df['asset'].map(config.RENAME_ETF)
            display_chart_assets(chart_settings, df, x_axis, y_axis, col1)

if option == 'Macroeconomic Data':
    #st.subheader(f'Macro Economic Data')
    option_indicator_type = st.sidebar.selectbox("Indicator Type", ('Lagging Indicator','Interest Rates','Leading Indicator'), 0)
    #logger = get_logger()

    if option_indicator_type == 'Lagging Indicator':
        st.subheader(f'Lagging Indicators')

        option_lagging_indicator_charts = st.sidebar.selectbox("Charts", ('002 - US GDP','005 - US Job Market','006 - PCE','007 - US Inflation','009 - US Industrial Production','011 - US Durable Goods', '011 - US Retail Sales'), 0)
        if option_lagging_indicator_charts == '002 - US GDP':    
            # gdpc1
            tab1, tab2, tab3, tab4 = st.tabs(["📈 Overall US GDP", "📈 US GDP QoQ", "📈 US GDP YoY", "📈 US GDP QoQ Annualized"])

            df_us_gdp_all, df_us_gdp_recent = get_stlouisfed_data('gdpc1', 'Q', 10)

            #TAB 1
            tab1.subheader("Overall GDP")
            col1, col2 = tab1.columns(2)

            series = "gdpc1"
            chart_settings = {
                "type": "line",
                "title": "Total US GDP", 
                "xlabel": "Year", 
                "ylabel": "GDP", 
                "ypercentage": False,
            }

            display_chart(chart_settings, df_us_gdp_all, series, tab1, col=col1)

            chart_settings = {
                "type": "line",
                "title": "Total US GDP - Last 10 Years", 
                "xlabel": "Year", 
                "ylabel": "GDP", 
                "ypercentage": False,

            }

            display_chart(chart_settings, df_us_gdp_recent, series, tab1, col=col2)
            
            rename_cols = {'DATE': 'Date', 'gdpc1': 'GDP'}
            cols_gradient = ['GDP']
            cols_drop = ['QoQ','YoY','QoQ_ANNUALIZED']
            format_cols = {
                'GDP': '{:,.2f}'.format,
                'Date': lambda t: t.strftime("%m-%d-%Y"),
            }
            format_date = True

            disp,df = style_df_for_display_date(df_us_gdp_recent,cols_gradient,rename_cols,cols_drop,format_cols)
            tab1.markdown(disp.to_html(), unsafe_allow_html=True)

            #TAB 2

            series = "QoQ"
            tab2.subheader("US GDP - QoQ")
            col1, col2 = tab2.columns(2)

            chart_settings = {
                "type": "bar",
                "title": "US GDP QoQ", 
                "xlabel": "Year", 
                "ylabel": "GDP QoQ", 
                "ypercentage": True,

            }

            display_chart(chart_settings, df_us_gdp_all, series, tab2,col=col1)

            chart_settings = {
                "type": "bar",
                "title": "US GDP QoQ - Last 10 Years", 
                "xlabel": "Year", 
                "ylabel": "GDP QoQ", 
                "ypercentage": True,

            }

            display_chart(chart_settings, deepcopy(df_us_gdp_recent), series, tab2,col=col2)

            cols_gradient = ['QoQ']
            rename_cols = {'DATE': 'Date'}
            cols_drop = ['gdpc1','YoY','QoQ_ANNUALIZED']
            format_cols = {
                'QoQ': '{:,.2%}'.format,
                'Date': lambda t: t.strftime("%m-%d-%Y"),
            }

            #import pdb; pdb.set_trace()
            #disp = df_us_gdp_recent.style.format(format_cols)

            disp,df = style_df_for_display_date(df_us_gdp_recent,cols_gradient,rename_cols,cols_drop,format_cols)
            tab2.markdown(disp.to_html(), unsafe_allow_html=True)

            #TAB 3
            series = "YoY"
            tab3.subheader("US GDP - YoY")
            col1, col2 = tab3.columns(2)

            chart_settings = {
                "type": "bar",
                "title": "US GDP YoY", 
                "xlabel": "Year", 
                "ylabel": "GDP YoY",
                "ypercentage": True,

            }

            display_chart(chart_settings, df_us_gdp_all, series, tab3, col=col1)

            chart_settings = {
                "type": "bar",
                "title": "US GDP YoY - Last 10 Years", 
                "xlabel": "Year", 
                "ylabel": "GDP YoY", 
                "ypercentage": True,

            }

            display_chart(chart_settings, deepcopy(df_us_gdp_recent), series, tab3, col=col2)

            cols_gradient = ['YoY']
            rename_cols = {'DATE': 'Date'}
            cols_drop = ['gdpc1','QoQ','QoQ_ANNUALIZED']
            format_cols = {
                'YoY': '{:,.2%}'.format,
                'Date': lambda t: t.strftime("%m-%d-%Y"),
            }

            disp,df = style_df_for_display_date(df_us_gdp_recent,cols_gradient,rename_cols,cols_drop,format_cols)
            tab3.markdown(disp.to_html(), unsafe_allow_html=True)

            #TAB 4
            series = "QoQ_ANNUALIZED"
            tab4.subheader("US GDP - QoQ Annualized")
            col1, col2 = tab4.columns(2)
            chart_settings = {
                "type": "bar",
                "title": "US GDP QoQ Annualized", 
                "xlabel": "Year", 
                "ylabel": "GDP QoQ Annualized", 
                "ypercentage": True,

            }

            display_chart(chart_settings, df_us_gdp_all, series, tab4, col=col1)

            chart_settings = {
                "type": "bar",
                "title": "US GDP QoQ Annualized - Last 10 Years", 
                "xlabel": "Year", 
                "ylabel": "GDP QoQ Annualized", 
                "ypercentage": True,

            }

            display_chart(chart_settings, deepcopy(df_us_gdp_recent), series, tab4, col=col2)

            rename_cols = {'DATE': 'Date','QoQ_ANNUALIZED':'QoQ Annualized'}
            cols_gradient = ['QoQ Annualized']
            cols_drop = ['gdpc1','QoQ','YoY']
            format_cols = {
                'QoQ Annualized': '{:,.2%}'.format,
                'Date': lambda t: t.strftime("%m-%d-%Y"),
            }
            disp,df = style_df_for_display_date(df_us_gdp_recent,cols_gradient,rename_cols,cols_drop,format_cols)
            tab4.markdown(disp.to_html(), unsafe_allow_html=True)


        if option_lagging_indicator_charts == '005 - US Job Market':    
            tab1, tab2, tab3, tab4 = st.tabs(["📈 US NFP", "📈 US Jobless Claims", "📈 Graphs", "📈 US ADP"])

            #TAB 1
            tab1.subheader("Non-Farm Payroll")
            col1, col2 = tab1.columns(2)

            # NFP	
            df_us_payems_all, df_us_payems_recent = get_stlouisfed_data('payems', 'M', 10)
            cols_drop = ['QoQ_ANNUALIZED','QoQ','YoY','MoM']            
            df_us_payems_recent = df_us_payems_recent.drop(cols_drop, axis=1)

            # Unemployment Rate
            df_us_unrate_all, df_us_unrate_recent = get_stlouisfed_data('unrate', 'M', 10)
            cols_drop = ['QoQ_ANNUALIZED','QoQ','YoY','MoM']            
            df_us_unrate_recent = df_us_unrate_recent.drop(cols_drop, axis=1)

            # Participation Rate	
            df_us_civpart_all, df_us_civpart_recent = get_stlouisfed_data('civpart', 'M', 10)
            cols_drop = ['QoQ_ANNUALIZED','QoQ','YoY','MoM']            
            df_us_civpart_recent = df_us_civpart_recent.drop(cols_drop, axis=1)

            df_us_payems_recent = combine_df_on_index(df_us_payems_recent, df_us_unrate_recent, 'DATE')
            df_us_payems_recent = combine_df_on_index(df_us_payems_recent, df_us_civpart_recent, 'DATE')
            df_us_payems_recent['unrate'] = df_us_payems_recent['unrate']/100
            df_us_payems_recent['diff'] = (df_us_payems_recent['payems'] - df_us_payems_recent['payems'].shift()) 

            #Monthly change in K, 3 month average, unemployment rate, participation rate
            df_us_payems_recent = df_us_payems_recent.loc[:, ['DATE','payems','diff','unrate','civpart']]

            # Add Charts
            series = "payems"

            chart_settings = {
                "type": "line",
                "title": "All Employees: Total Nonfarm Payrolls, Thousands of Persons, Monthly, Seasonally Adjusted", 
                "xlabel": "Period", 
                "ylabel": "Persons", 
                "ypercentage": False,

            }

            display_chart(chart_settings, df_us_payems_recent, series, tab1, col=col1)

            series = "diff"

            chart_settings = {
                "type": "line",
                "title": "Monthly Change in K", 
                "xlabel": "Period", 
                "ylabel": "Persons", 
                "ypercentage": False,

            }

            display_chart(chart_settings, df_us_payems_recent, series, tab1, col=col2)

            rename_cols = {'DATE': 'Date','diff':'Monthly Change (K)' ,'payems': 'NFP', 'unrate':'Unemployment Rate', 'civpart':'Participation Rate'}
            cols_gradient = ['Monthly Change (K)']
            cols_drop = []
            format_cols = {
                'NFP': '{:,.0f}'.format,
                'Unemployment Rate': '{:,.2%}'.format,
                'Participation Rate': '{:,.2f}'.format,
                'Date': lambda t: t.strftime("%m-%d-%Y"),
                'Monthly Change (K)': '{:,.0f}'.format,
            }
            disp,df = style_df_for_display_date(df_us_payems_recent,cols_gradient,rename_cols,cols_drop,format_cols)
            tab1.markdown(disp.to_html(), unsafe_allow_html=True)

            
            #TAB 2
            tab2.subheader("Jobless Claims")
            col1, col2 = tab2.columns(2)

            # Jobless Claims
            df_us_icsa_all, df_us_icsa_recent = get_stlouisfed_data('icsa', 'M', 10)
            cols_drop = ['QoQ_ANNUALIZED','QoQ','YoY','MoM']            
            df_us_icsa_recent = df_us_icsa_recent.drop(cols_drop, axis=1)
            df_us_icsa_recent['icsa_var'] = (df_us_icsa_recent['icsa'] - df_us_icsa_recent['icsa'].shift()) 

            df_us_ccsa_all, df_us_ccsa_recent = get_stlouisfed_data('ccsa', 'M', 10)
            df_us_ccsa_recent = df_us_ccsa_recent.drop(cols_drop, axis=1)
            df_us_ccsa_recent['ccsa_var'] = (df_us_ccsa_recent['ccsa'] - df_us_ccsa_recent['ccsa'].shift()) 

            df_us_icsa_recent = combine_df_on_index(df_us_icsa_recent, df_us_ccsa_recent, 'DATE')
            df_us_icsa_recent = df_us_icsa_recent.loc[:, ['DATE','icsa','icsa_var','ccsa','ccsa_var']]

            df_us_icsa_100_periods = df_us_icsa_recent.tail(100)

            series = "icsa"
            chart_settings = {
                "type": "line",
                "title": "Initial Claims", 
                "xlabel": "Period", 
                "ylabel": "Persons", 
                "ypercentage": False,

            }

            display_chart(chart_settings, df_us_icsa_recent, series, tab2,col=col1)
            display_chart(chart_settings, df_us_icsa_100_periods, series, tab2, col=col2)

            rename_cols = {'DATE': 'Date','icsa':'Initial Claims' ,'ccsa': 'Continued Claims', 'icsa_var':'IC Var', 'ccsa_var':'CC Var'}
            cols_gradient = ['Initial Claims']
            cols_drop = []
            format_cols = {
                'Initial Claims': '{:,.0f}'.format,
                'IC Var': '{:,.0f}'.format,
                'Continued Claims': '{:,.0f}'.format,
                'CC Var': '{:,.0f}'.format,
                'Date': lambda t: t.strftime("%m-%d-%Y"),
            }
            disp,df = style_df_for_display_date(df_us_icsa_recent,cols_gradient,rename_cols,cols_drop,format_cols)
            tab2.markdown(disp.to_html(), unsafe_allow_html=True)

            #TAB 3
            tab3.subheader("Graphs")
            col1, col2 = tab3.columns(2)

            series = "payems"
            chart_settings = {
                "type": "line",
                "title": "NFP", 
                "xlabel": "Year", 
                "ylabel": "Total NFP", 
                "ypercentage": False,

            }

            display_chart(chart_settings, df_us_payems_all, series, tab3, col=col1)

            #df_us_unrate_all
            series = "unrate"
            chart_settings = {
                "type": "line",
                "title": "Unemployment Rate", 
                "xlabel": "Year", 
                "ylabel": "Unemployed %", 
                "ypercentage": False,

            }

            display_chart(chart_settings, df_us_unrate_all, series, tab3, col=col2)

            #df_us_civpart_all
            series = "civpart"
            chart_settings = {
                "type": "line",
                "title": "Labour Participation Rate", 
                "xlabel": "Year", 
                "ylabel": "Participation %", 
                "ypercentage": False,

            }

            display_chart(chart_settings, df_us_civpart_all, series, tab3, col=col1)

            #TAB 4
            tab4.subheader("ADP National Employment Report")
            #ADP = ADP
            #TODO: Display the appropriate charts and tables
            #df_us_payems_all, df_us_payems_recent = get_stlouisfed_data('payems', 'Q', 10)


        if option_lagging_indicator_charts == '006 - PCE':

            tab1, tab2, tab3 = st.tabs(["📈 PCE Deflator", "📈 PCE Core", "📈 PCE Core vs Core CPI"])

            # Display the appropriate charts and tables
            df_us_dfedtaru_all, df_us_dfedtaru_recent = get_stlouisfed_data('dfedtaru', 'M',10)
            df_us_pcepilfe_all, df_us_pcepilfe_recent = get_stlouisfed_data('pcepilfe', 'M',10)           

            #TAB 1
            col1, col2 = tab1.columns(2)
            df_us_pcepi_all, df_us_pcepi_recent = standard_display('pcepi', tab1,'PCE Deflator','M','YoY',col1=col1,col2=col2)

            #TAB 2
            col1, col2 = tab1.columns(2)

            tab2.subheader("PCE Core")
            col1, col2 = tab2.columns(2)
            df_us_pcepilfe_all["target_rate_percent"] = 2
            df_us_pcepilfe_recent["target_rate_percent"] = 2
            series2 = "target_rate_percent"

            series = "YoY"
            chart_settings = {
                "type": "line",
                "title": "PCE Core YoY", 
                "xlabel": "Year", 
                "ylabel": "YoY Change", 
                "ypercentage": True,
            }

            display_chart(chart_settings, df_us_pcepilfe_all, series, tab2, series2,col=col1)

            # Superimpose df_us_dfedtaru_all into chart as well as table
            cols_drop = ['QoQ_ANNUALIZED','QoQ','YoY','MoM']            
            df_us_dfedtaru_recent = df_us_dfedtaru_recent.drop(cols_drop, axis=1)
            df_us_pcepilfe_recent = append_two_df(df_us_pcepilfe_recent,df_us_dfedtaru_recent, 'inner')
            series2 = 'dfedtaru'

            chart_settings = {
                "type": "line",
                "title": "PCE Core YoY - Last 10 Years", 
                "xlabel": "Year", 
                "ylabel": "YoY Change", 
                "ypercentage": True,
            }

            display_chart(chart_settings, deepcopy(df_us_pcepilfe_recent), series, tab2, series2,col=col2)
            
            rename_cols = {'DATE': 'Date (MM-DD-YYYY)', 'pcepilfe': 'PCE Core', 'target_rate_percent': 'Fed Target', 'dfedtaru': 'Fed Fund Target'}
            cols_gradient = ['YoY']
            cols_drop = ['QoQ','QoQ_ANNUALIZED']
            format_cols = {
                'Fed Target': '{:,.2f}%'.format,
                'Fed Fund Target': '{:,.2f}%'.format,
                'MoM': '{:,.2%}'.format,
                'YoY': '{:,.2%}'.format,
                'PCE': '{:,.2f}'.format,
                'Date (MM-DD-YYYY)': lambda t: t.strftime("%m-%d-%Y"),
            }

            disp,df = style_df_for_display_date(df_us_pcepilfe_recent,cols_gradient,rename_cols,cols_drop,format_cols)
            tab2.markdown(disp.to_html(), unsafe_allow_html=True)           

            #TAB 3
            tab3.subheader("PCE Core vs Core CPI")
            #TODO

        if option_lagging_indicator_charts == '007 - US Inflation':
            tabs_list = ["📈 CPI", "📈 CPI Food", "📈 CPI Energy", "📈 CPI Core"]
            tab1, tab2, tab3, tab4 = st.tabs(tabs_list)

            # Display the appropriate charts and tables
            #TAB1
            col1, col2 = tab1.columns(2)            
            df_us_cpiaucsl_all, df_us_cpiaucsl_recent = standard_display('cpiaucsl', tab1, 'CPI', 'M','YoY',col1=col1,col2=col2)

            #TAB2
            col1, col2 = tab2.columns(2)
            df_us_cpifabsl_all, df_us_cpifabsl_recent = standard_display('cpifabsl', tab2, 'CPI Food & Beverages', 'M','YoY',col1=col1,col2=col2)

            #TAB3
            col1, col2 = tab3.columns(2)
            df_us_cpiengsl_all, df_us_cpiengsl_recent = standard_display('cpiengsl', tab3, 'CPI Energy', 'M','YoY',col1=col1,col2=col2)

            #TAB4
            col1, col2 = tab4.columns(2)
            df_us_cpilfesl_all, df_us_cpilfesl_recent = standard_display('cpilfesl', tab4, 'CPI Core', 'M','YoY',col1=col1,col2=col2)

        if option_lagging_indicator_charts == '009 - US Industrial Production':

            tabs_list = ["📈 IP from start", 
                        "📈 Industrial Production", 
                        "📈 Capacity Utilization", 
                        "📈 Materials", 
                        "📈 Consumer Goods", 
                        "📈 Business Equipment", 
                        "📈 Construction", 
                        "📈 Manu", 
                        "📈 Mining", 
                        "📈 Utilities"]
            tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9, tab10 = st.tabs(tabs_list)

            #TAB1
            col1, col2 = tab1.columns(2)
            df_indpro_all, df_indpro_recent = standard_display('indpro', tab1, 'IP From Start', 'M','YoY',col1=col1,col2=col2)

            #TAB2
            col1, col2 = tab2.columns(2)            
            df_us_ipb54100s_all, df_us_ipb54100s_recent = standard_display('ipb54100s', tab2,'Industrial Production','M','YoY',col1=col1,col2=col2)

            #TAB3
            col1, col2 = tab3.columns(2)            
            df_us_ipbuseq_all, df_us_ipbuseq_recent = standard_display('ipbuseq', tab3,'Capacity Utilization','M','YoY',col1=col1,col2=col2)

            #TAB4
            col1, col2 = tab4.columns(2)            
            df_us_ipcongd_all, df_us_ipcongd_recent = standard_display('ipcongd', tab4,'Materials','M','YoY',col1=col1,col2=col2)

            #TAB5
            col1, col2 = tab5.columns(2)            
            df_us_ipman_all, df_us_ipman_recent = standard_display('ipman', tab5,'Consumer Goods','M','YoY',col1=col1,col2=col2)

            #TAB6
            col1, col2 = tab6.columns(2)
            df_us_ipmat_all, df_us_ipmat_recent = standard_display('ipmat', tab6,'Business Equipment','M','YoY',col1=col1,col2=col2)

            #TAB7
            col1, col2 = tab7.columns(2)
            df_us_ipmine_all, df_us_ipmine_recent = standard_display('ipmine', tab7,'Construction','M','YoY',col1=col1,col2=col2)

            #TAB8
            col1, col2 = tab8.columns(2)
            df_us_iputil_all, df_us_iputil_recent = standard_display('iputil', tab8,'Manufacturing','M','YoY',col1=col1,col2=col2)

            #TAB9
            col1, col2 = tab9.columns(2)
            df_us_tcu_all, df_us_tcu_recent = standard_display('tcu', tab9,'Mining','M','YoY',col1=col1,col2=col2)

            #TAB10
            col1, col2 = tab10.columns(2)
            df_us_wpsfd4131_all, df_us_wpsfd4131_recent = standard_display('wpsfd4131', tab10,'Utilities','M','YoY',col1=col1,col2=col2)

        if option_lagging_indicator_charts == '011 - US Durable Goods':
            tabs_list = ["📈 Recap", 
                        "📈 New Orders", 
                        "📈 New Orders ex Aircraft (Core Orders)", 
                        "📈 New Orders ex Transport", 
                        "📈 Other (Manufacturing)"]
            tab1, tab2, tab3, tab4, tab5 = st.tabs(tabs_list)

            #TODO: TAB1

            #TAB2
            col1, col2 = tab2.columns(2)
            df_dgorder_all, df_dgorder_recent = standard_display('dgorder', tab2, 'New Orders', 'M', 'dgorder',col1=col1,col2=col2)

            #TAB3
            col1, col2 = tab3.columns(2)
            df_neworder_all, df_neworder_recent = standard_display('neworder', tab3, 'New Orders ex Aircraft', 'M', 'neworder',col1=col1,col2=col2)

            #TAB4
            col1, col2 = tab4.columns(2)
            df_adxtno_all, df_adxtno_recent = standard_display('adxtno', tab4, 'New Orders ex Transport', 'M', 'adxtno',col1=col1,col2=col2)

            #TAB5
            col1, col2 = tab5.columns(2)
            df_amtuno_all, df_amtuno_recent = standard_display('amtuno', tab5, 'New Orders Manufacturing', 'M', 'amtuno',col1=col1,col2=col2)

        if option_lagging_indicator_charts == '011 - US Retail Sales':
            tabs_list = ["📈 Recap", 
                        "📈 Recap 2", 
                        "📈 Retail Sales ex Auto and Gas", 
                        "📈 Retail Sales", 
                        "📈 Retail Sales ex Auto",
                        "📈 Food and Beverage",
                        "📈 Non Store Retail",
                        "📈 Health",
                        "📈 Sporting Goods",
                        "📈 General Merchandising",
                        "📈 Food Servies",
                        "📈 Gas Station",
                        "📈 Motor",
                        "📈 Housing",
                        "📈 Furniture Home",
                        "📈 Miscellaneous Stores Retailers",
                        "📈 Clothing and Clothing Access"]
            
            tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9, tab10, tab11, tab12, tab13, tab14, tab15, tab16, tab17 = st.tabs(tabs_list)

            #TODO: TAB1

            #TODO: TAB2

            # TAB3 #MARTSSM44W72USS
            col1, col2 = tab3.columns(2)
            series = 'MARTSSM44W72USS'.lower()
            df_MARTSSM44W72USS_all, df_MARTSSM44W72USS_recent = standard_display(series, tab3, 'Retail Sales ex Auto and Gas', 'M', series,col1=col1,col2=col2)

            # TAB4 #RSAFS
            col1, col2 = tab4.columns(2)
            series = 'RSAFS'.lower()
            df_RSAFS_all, df_RSAFS_recent = standard_display(series, tab4, 'Retail Sales', 'M', series,col1=col1,col2=col2)

            # TAB5
            col1, col2 = tab5.columns(2)
            series = 'RSFSXMV'.lower()
            df_RSFSXMV_all, df_RSFSXMV_recent = standard_display(series, tab5, 'Retail Sales ex Auto', 'M', series,col1=col1,col2=col2)

            # TAB6 
            col1, col2 = tab6.columns(2)
            series = 'RSDBS'.lower()
            df_RSDBS_all, df_RSDBS_recent = standard_display(series, tab6, 'Food and Beverage', 'M', series,col1=col1,col2=col2)

            # TAB7 #RSNSR
            col1, col2 = tab7.columns(2)
            series = 'RSNSR'.lower()
            df_RSNSR_all, df_RSNSR_recent = standard_display(series, tab7, 'Non Store Retail', 'M', series,col1=col1,col2=col2)

            # TAB8 #RSHPCS
            col1, col2 = tab8.columns(2)
            series = 'RSHPCS'.lower()
            df_RSHPCS_all, df_RSHPCS_recent = standard_display(series, tab8, 'Health', 'M', series,col1=col1,col2=col2)

            # TAB9 #RSSGHBMS
            col1, col2 = tab9.columns(2)
            series = 'RSSGHBMS'.lower()
            df_RSSGHBMS_all, df_RSSGHBMS_recent = standard_display(series, tab9, 'Sporting Goods', 'M', series,col1=col1,col2=col2)

            # TAB10 #RSGMS	
            col1, col2 = tab10.columns(2)
            series = 'RSGMS'.lower()
            df_RSGMS_all, df_RSGMS_recent = standard_display(series, tab10, 'General Merchandising', 'M', series,col1=col1,col2=col2)

            # TAB11 #RSFSDP
            col1, col2 = tab11.columns(2)
            series = 'RSFSDP'.lower()
            df_RSFSDP_all, df_RSFSDP_recent = standard_display(series, tab11, 'Food Services', 'M', series,col1=col1,col2=col2)

            # TAB12 #RSGASS	
            col1, col2 = tab12.columns(2)
            series = 'RSGASS'.lower()
            df_RSGASS_all, df_RSGASS_recent = standard_display(series, tab12, 'Gas Station', 'M', series,col1=col1,col2=col2)

            # TAB13 #RSMVPD
            col1, col2 = tab13.columns(2)
            series = 'RSMVPD'.lower()
            df_RSMVPD_all, df_RSMVPD_recent = standard_display(series, tab13, 'Motor', 'M', series,col1=col1,col2=col2)

            # TAB14 #RSBMGESD
            col1, col2 = tab14.columns(2)
            series = 'RSBMGESD'.lower()
            df_RSBMGESD_all, df_RSBMGESD_recent = standard_display(series, tab14, 'Housing', 'M', series,col1=col1,col2=col2)

            # TAB15 #RSFHFS
            col1, col2 = tab15.columns(2)
            series = 'RSFHFS'.lower()
            df_RSFHFS_all, df_RSFHFS_recent = standard_display(series, tab15, 'Furniture Home', 'M', series,col1=col1,col2=col2)

            # TAB16 #RSMSR
            col1, col2 = tab16.columns(2)
            series = 'RSMSR'.lower()
            df_RSMSR_all, df_RSMSR_recent = standard_display(series, tab16, 'Miscellaneous Stores Retailers', 'M', series,col1=col1,col2=col2)

            # TAB17 #RSCCAS
            col1, col2 = tab17.columns(2)
            series = 'RSCCAS'.lower()
            df_RSCCAS_all, df_RSCCAS_recent = standard_display(series, tab17, 'Clothing and Clothing Accessories', 'M', series,col1=col1,col2=col2)
                        	
            #RSEAS	
            		
    if option_indicator_type == 'Interest Rates':
        st.subheader(f'Interest Rates')
        df_interest_rates_10y = get_data(table="macro_ir_10y")           
        df_us_treasury_yields = get_data(table="macro_ustreasuryyields") 

        option_interest_rates_charts = st.sidebar.selectbox("Charts", ('013 - Interest Rates','013 - Yield Curve'), 0)
        if option_interest_rates_charts == '013 - Interest Rates':    
            df_interest_rates_10y = df_interest_rates_10y.sort_values('dt').fillna(method='ffill')           
            rename_cols = {'australia':'Australia','brazil':'Brazil','canada':'Canada','china':'China','france':'France','germany':'Germany', 'uk': 'United Kingdom', 'us': 'United States'}
            df_interest_rates_10y = df_interest_rates_10y.rename(columns=rename_cols)
            #import pdb; pdb.set_trace()
            df_interest_rates_10y_australia = calc_ir_metrics(df_interest_rates_10y[["dt", "Australia"]])
            df_interest_rates_10y_brazil = calc_ir_metrics(df_interest_rates_10y[["dt", "Brazil"]])
            df_interest_rates_10y_canada = calc_ir_metrics(df_interest_rates_10y[["dt", "Canada"]])
            df_interest_rates_10y_china = calc_ir_metrics(df_interest_rates_10y[["dt", "China"]])
            df_interest_rates_10y_france = calc_ir_metrics(df_interest_rates_10y[["dt", "France"]])
            df_interest_rates_10y_germany = calc_ir_metrics(df_interest_rates_10y[["dt", "Germany"]])
            df_interest_rates_10y_uk = calc_ir_metrics(df_interest_rates_10y[["dt", "United Kingdom"]])
            df_interest_rates_10y_us = calc_ir_metrics(df_interest_rates_10y[["dt", "United States"]])

            df_10y = df_interest_rates_10y_australia.append(df_interest_rates_10y_brazil,ignore_index = True) 
            df_10y = df_10y.append(df_interest_rates_10y_canada,ignore_index = True) 
            df_10y = df_10y.append(df_interest_rates_10y_china,ignore_index = True) 
            df_10y = df_10y.append(df_interest_rates_10y_france,ignore_index = True) 
            df_10y = df_10y.append(df_interest_rates_10y_germany,ignore_index = True) 
            df_10y = df_10y.append(df_interest_rates_10y_uk,ignore_index = True) 
            df_10y = df_10y.append(df_interest_rates_10y_us,ignore_index = True) 
            
            df_10y['Last Date'] = pd.to_datetime(df_10y['Last Date'],format='%Y-%m-%d')
            df_10y['Last'] = pd.to_numeric(df_10y['Last'])
            df_10y['1w'] = pd.to_numeric(df_10y['1w'])
            df_10y['1m'] = pd.to_numeric(df_10y['1m'])
            df_10y['3m'] = pd.to_numeric(df_10y['3m'])
            df_10y['YTD'] = pd.to_numeric(df_10y['YTD'])
            df_10y['YoY'] = pd.to_numeric(df_10y['YoY'])

            df_interest_rates_2y = get_data(table="macro_ir_2y")          
            df_interest_rates_2y = df_interest_rates_2y.sort_values('dt').fillna(method='ffill')           
            rename_cols = {'australia':'Australia','brazil':'Brazil','canada':'Canada','china':'China','france':'France','germany':'Germany', 'uk': 'United Kingdom', 'us': 'United States'}
            df_interest_rates_2y = df_interest_rates_2y.rename(columns=rename_cols)

            df_interest_rates_2y_australia = calc_ir_metrics(df_interest_rates_2y[["dt", "Australia"]])
            df_interest_rates_2y_brazil = calc_ir_metrics(df_interest_rates_2y[["dt", "Brazil"]])
            df_interest_rates_2y_canada = calc_ir_metrics(df_interest_rates_2y[["dt", "Canada"]])
            df_interest_rates_2y_china = calc_ir_metrics(df_interest_rates_2y[["dt", "China"]])
            df_interest_rates_2y_france = calc_ir_metrics(df_interest_rates_2y[["dt", "France"]])
            df_interest_rates_2y_germany = calc_ir_metrics(df_interest_rates_2y[["dt", "Germany"]])
            df_interest_rates_2y_uk = calc_ir_metrics(df_interest_rates_2y[["dt", "United Kingdom"]])
            df_interest_rates_2y_us = calc_ir_metrics(df_interest_rates_2y[["dt", "United States"]])

            df_2y = df_interest_rates_2y_australia.append(df_interest_rates_2y_brazil,ignore_index = True) 
            df_2y = df_2y.append(df_interest_rates_2y_canada,ignore_index = True) 
            df_2y = df_2y.append(df_interest_rates_2y_china,ignore_index = True) 
            df_2y = df_2y.append(df_interest_rates_2y_france,ignore_index = True) 
            df_2y = df_2y.append(df_interest_rates_2y_germany,ignore_index = True) 
            df_2y = df_2y.append(df_interest_rates_2y_uk,ignore_index = True) 
            df_2y = df_2y.append(df_interest_rates_2y_us,ignore_index = True) 

            df_2y['Last Date'] = pd.to_datetime(df_2y['Last Date'],format='%Y-%m-%d')
            df_2y['Last'] = pd.to_numeric(df_2y['Last'])
            df_2y['1w'] = pd.to_numeric(df_2y['1w'])
            df_2y['1m'] = pd.to_numeric(df_2y['1m'])
            df_2y['3m'] = pd.to_numeric(df_2y['3m'])
            df_2y['YTD'] = pd.to_numeric(df_2y['YTD'])
            df_2y['YoY'] = pd.to_numeric(df_2y['YoY'])

            # Calculate 10y - 2y values
            data = {'Country': [],'Last': [],'1w': [],'1m': [],'3m': [],'YTD': [],'YoY': []}

            # Convert the dictionary into DataFrame
            df_10y_minus_2y = pd.DataFrame(data)

            df_10y_minus_2y['Country'] = ['Australia','Brazil','Canada','China','France','Germany','United Kingdom','United States']
            df_10y_minus_2y['Last'] = df_10y['Last'] - df_2y['Last']
            df_10y_minus_2y['1w'] = df_10y['1w'] - df_2y['1w']
            df_10y_minus_2y['1m'] = df_10y['1m'] - df_2y['1m']
            df_10y_minus_2y['3m'] = df_10y['3m'] - df_2y['3m']
            df_10y_minus_2y['YTD'] = df_10y['YTD'] - df_2y['YTD']
            df_10y_minus_2y['YoY'] = df_10y['YoY'] - df_2y['YoY']

            st.markdown('10Y Interest Rates')

            rename_cols = {}
            #cols_gradient = ['Last','1w','1m','3m','YTD','YoY']
            cols_gradient = ['1w','1m','YoY']
            cols_drop = []
            format_cols = {
                'Last': '{:,.2f}'.format,
                '1w': '{:,.2f}'.format,
                '1m': '{:,.2f}'.format,
                '3m': '{:,.2f}'.format,
                'YTD': '{:,.2f}'.format,
                'YoY': '{:,.2f}'.format,
                'Last Date': lambda t: t.strftime("%d-%m-%Y"),                
            }

            disp,df = style_df_for_display(df_10y,cols_gradient,rename_cols,cols_drop,format_cols)
            st.markdown(disp.to_html(), unsafe_allow_html=True)           

            st.markdown("""---""")

            st.markdown('2Y Interest Rates')

            rename_cols = {}
            #cols_gradient = ['Last','1w','1m','3m','YTD','YoY']
            cols_gradient = ['1w','1m','YoY']
            cols_drop = []
            format_cols = {
                'Last': '{:,.2f}'.format,
                '1w': '{:,.2f}'.format,
                '1m': '{:,.2f}'.format,
                '3m': '{:,.2f}'.format,
                'YTD': '{:,.2f}'.format,
                'YoY': '{:,.2f}'.format,
                'Last Date': lambda t: t.strftime("%d-%m-%Y"),                
            }

            disp,df = style_df_for_display(df_2y,cols_gradient,rename_cols,cols_drop,format_cols)
            st.markdown(disp.to_html(), unsafe_allow_html=True)           

            st.markdown("""---""")

            st.markdown('10Y - 2Y Interest Rates')

            rename_cols = {}
            #cols_gradient = ['Last','1w','1m','3m','YTD','YoY']
            cols_gradient = ['1w','1m','YoY']
            cols_drop = []
            format_cols = {
                'Last': '{:,.2f}'.format,
                '1w': '{:,.2f}'.format,
                '1m': '{:,.2f}'.format,
                '3m': '{:,.2f}'.format,
                'YTD': '{:,.2f}'.format,
                'YoY': '{:,.2f}'.format,
                'Last Date': lambda t: t.strftime("%d-%m-%Y"),                
            }

            disp,df = style_df_for_display(df_10y_minus_2y,cols_gradient,rename_cols,cols_drop,format_cols)
            st.markdown(disp.to_html(), unsafe_allow_html=True)          

            # Get Company Credit Ratings
            st.markdown("""---""")
            st.markdown('Country Ratings')

            df_country_ratings = get_data(table="macro_countryratings")           
            # Filter by certain countries only
            countries_list = ['Australia','Brazil','Canada','China','France','Germany', 'United Kingdom', 'United States'] #{'australia':'Australia','brazil':'Brazil','canada':'Canada','china':'China','france':'France','germany':'Germany', 'uk': 'United Kingdom', 'us': 'United States'}
            # selecting rows based on condition 
            df_countries_list_filtered = df_country_ratings[df_country_ratings['country'].isin(countries_list)]
            df_countries_list_filtered = df_countries_list_filtered.sort_values(by=['country'], ascending=True)
            rename_cols = {
                'country':'Country',	
                's_and_p':'S&P',	
                'moodys':'Moodys',	
                'dbr':'DBR',
            }
            cols_gradient = []
            cols_drop = ['id',]
            format_cols = {}

            disp,df = style_df_for_display(df_countries_list_filtered,cols_gradient,rename_cols,cols_drop,format_cols)
            st.markdown(disp.to_html(), unsafe_allow_html=True)          
        if option_interest_rates_charts == '013 - Yield Curve':    
            df_us_treasury_yields = df_us_treasury_yields.rename(columns={"dt": "DATE"}) 
            df_us_treasury_yields['DATE'] = pd.to_datetime(df_us_treasury_yields['DATE'],format='%Y-%m-%d')
            df_us_treasury_yields['10Y-2Y'] = df_us_treasury_yields['rate10y'] - df_us_treasury_yields['rate2y']
            df_us_treasury_yields['10Y-3Y'] = df_us_treasury_yields['rate10y'] - df_us_treasury_yields['rate3y']         
            #df_us_treasury_yields['10Y-2Y'] = pd.to_numeric(df_us_treasury_yields['10Y-2Y'])
            #df_us_treasury_yields['10Y-3Y'] = pd.to_numeric(df_us_treasury_yields['10Y-3Y'])

            df_display = df_us_treasury_yields[["DATE", "10Y-2Y", "10Y-3Y"]]
            tabs_list = ["📈 Yield Curve 2Y-10Y", 
                        "📈 Interest Rates Table"]
            
            tab1, tab2 = st.tabs(tabs_list)

            #TAB 1
            # Get the most recent data (ie. last 8 years)
            todays_date = date.today()
            start_date = todays_date - relativedelta(years=8)
            date_str_start = "%s-%s-%s" % (start_date.year, start_date.month, start_date.day)

            df_display_recent = df_display.loc[(df_display['DATE'] >= date_str_start)].reset_index(drop=True)            
            col1, col2 = tab1.columns(2)

            series = "10Y-2Y"
            chart_settings = {
                "type": "line",
                "title": "Yield Curve 10Y - 2Y", 
                "xlabel": "Date", 
                "ylabel": "Rate", 
                "ypercentage": False,
            }

            display_chart(chart_settings, df_display_recent, series, tab1, col=col1)

            series = "10Y-3Y"
            chart_settings = {
                "type": "line",
                "title": "Yield Curve 10Y - 3Y", 
                "xlabel": "Date", 
                "ylabel": "Rate", 
                "ypercentage": False,
            }

            display_chart(chart_settings, df_display_recent, series, tab1, col=col2)

            #TAB 2
            rename_cols = {'rate3m':'3 Month','rate2y':'2 Year','rate3y':'3 Year','rate10y':'10 Year','rate30y':'30 Year'}

            #cols_gradient = ['Last','1w','1m','3m','YTD','YoY']
            cols_gradient = []
            cols_drop = ['id','10Y-2Y','10Y-3Y']            
            format_cols = {
                'DATE': lambda t: t.strftime("%d-%m-%Y"),                
            }

            disp,df = style_df_for_display(df_us_treasury_yields.sort_values(by=['DATE'],ascending=False),cols_gradient,rename_cols,cols_drop,format_cols)
            tab2.markdown(disp.to_html(), unsafe_allow_html=True)           

    if option_indicator_type == 'Leading Indicator':
        st.subheader(f'Leading Indicators')

        option_leading_indicator_charts = st.sidebar.selectbox("Charts", ('016 - US ISM Manufacturing','017 - US ISM Services'), 0)

        if option_leading_indicator_charts == '016 - US ISM Manufacturing':    
            tabs = ["📈 Sectors", 
                    "📈 New Orders", 
                    "📈 Production", 
                    "📈 Sector Trends", 
                    "📈 Details", 
                    "📈 Charts", 
                    "📈 Vs GDP"
                    ]

            tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(tabs)

            df_sectors = get_data(table="macro_us_ism_manufacturing_sectors").tail(6).reset_index(drop=True)           
            df_new_orders = get_data(table="macro_us_ism_manufacturing_new_orders").tail(6).reset_index(drop=True)            
            df_production = get_data(table="macro_us_ism_manufacturing_production").tail(6).reset_index(drop=True)            
            df_headline = get_data(table="macro_us_ism_manufacturing_headline").tail(24).reset_index(drop=True).sort_values(by=['ism_date'], ascending=False)            

            df_sectors_last_6_months = df_sectors.sort_values('ism_date').tail(3).reset_index(drop=True)
            df_new_orders_last_6_months = df_new_orders.sort_values('ism_date').tail(3).reset_index(drop=True)
            df_production_last_6_months = df_production.sort_values('ism_date').tail(3).reset_index(drop=True)

            #TAB 1
            tab1.subheader(tabs[0])

            rename_cols = {'ism_date': 'DATE','apparel_leather_allied_products':'Apparel Leather Allied Products','chemical_products':'Chemical Products','computer_electronic_products':'Computer & Electronic Products','electrical_equipment_appliances_components':'Electrical Equipment Appliances Components','fabricated_metal_products':'Fabricated Metal Products','food_beverage_tobacco_products':'Food Beverage & Tobacco Products','furniture_related_products':'Furniture Related Products','machinery':'Machinery','miscellaneous_manufacturing':'Miscellaneous Manufacturing','nonmetallic_mineral_products':'Nonmetallic Mineral Products','paper_products':'Paper Products','petroleum_coal_products':'Petroleum Coal Products','plastics_rubber_products':'Plastics & Rubber Products','primary_metals':'Primary Metails','printing_related_support_activities':'Printing Related Support Activities','textile_mills':'Textile Mills','transportation_equipment':'Transportation Equipment','wood_products':'Wood Products'}
            cols_gradient = []
            cols_drop = []
            format_cols = {
                'DATE': lambda t: t.strftime("%m-%Y")
            }

            disp, df_sectors_last_6_months = style_df_for_display(df_sectors_last_6_months,cols_gradient,rename_cols,cols_drop,cols_format=format_cols,format_rows=True)
            tab1.markdown(disp.to_html(), unsafe_allow_html=True)           

            col1, col2, col3 = tab1.columns(3)

            index = 1
            for x, y in rename_cols.items():
                series = y
                if(y != 'DATE'):
                    chart_settings = {
                        "title": y, 
                        "xlabel": "Period", 
                        "ylabel": "Index", 
                        "ypercentage": False,
                    }

                    display_chart_ism(chart_settings, df_sectors_last_6_months, series, col=eval("col" + str(index)))

                    if(index == 3):
                        index = 1
                    else:
                        index = index + 1

            #TAB 2
            tab2.subheader(tabs[1])
            rename_cols = {'ism_date': 'DATE','apparel_leather_allied_products':'Apparel Leather Allied Products','chemical_products':'Chemical Products','computer_electronic_products':'Computer & Electronic Products','electrical_equipment_appliances_components':'Electrical Equipment Appliances Components','fabricated_metal_products':'Fabricated Metal Products','food_beverage_tobacco_products':'Food Beverage & Tobacco Products','furniture_related_products':'Furniture Related Products','machinery':'Machinery','miscellaneous_manufacturing':'Miscellaneous Manufacturing','nonmetallic_mineral_products':'Nonmetallic Mineral Products','paper_products':'Paper Products','petroleum_coal_products':'Petroleum Coal Products','plastics_rubber_products':'Plastics & Rubber Products','primary_metals':'Primary Metails','printing_related_support_activities':'Printing Related Support Activities','textile_mills':'Textile Mills','transportation_equipment':'Transportation Equipment','wood_products':'Wood Products'}
            cols_gradient = []
            cols_drop = []
            format_cols = {
                'DATE': lambda t: t.strftime("%m-%Y")
            }

            disp, df_new_orders_last_6_months = style_df_for_display(df_new_orders_last_6_months,cols_gradient,rename_cols,cols_drop,cols_format=format_cols,format_rows=True)
            tab2.markdown(disp.to_html(), unsafe_allow_html=True)           

            col1, col2, col3 = tab2.columns(3)

            index = 1
            for x, y in rename_cols.items():
                series = y
                if(y != 'DATE'):
                    chart_settings = {
                        "title": y, 
                        "xlabel": "Period", 
                        "ylabel": "Index", 
                        "ypercentage": False,
                    }

                    display_chart_ism(chart_settings, df_new_orders_last_6_months, series, col=eval("col" + str(index)))

                    if(index == 3):
                        index = 1
                    else:
                        index = index + 1

            #TAB 3
            tab3.subheader(tabs[2])
            rename_cols = {'ism_date': 'DATE','apparel_leather_allied_products':'Apparel Leather Allied Products','chemical_products':'Chemical Products','computer_electronic_products':'Computer & Electronic Products','electrical_equipment_appliances_components':'Electrical Equipment Appliances Components','fabricated_metal_products':'Fabricated Metal Products','food_beverage_tobacco_products':'Food Beverage & Tobacco Products','furniture_related_products':'Furniture Related Products','machinery':'Machinery','miscellaneous_manufacturing':'Miscellaneous Manufacturing','nonmetallic_mineral_products':'Nonmetallic Mineral Products','paper_products':'Paper Products','petroleum_coal_products':'Petroleum Coal Products','plastics_rubber_products':'Plastics & Rubber Products','primary_metals':'Primary Metails','printing_related_support_activities':'Printing Related Support Activities','textile_mills':'Textile Mills','transportation_equipment':'Transportation Equipment','wood_products':'Wood Products'}
            cols_gradient = []
            cols_drop = []
            format_cols = {
                'DATE': lambda t: t.strftime("%m-%Y")
            }

            disp,df_production_last_6_months = style_df_for_display(df_production_last_6_months,cols_gradient,rename_cols,cols_drop,cols_format=format_cols,format_rows=True)
            tab3.markdown(disp.to_html(), unsafe_allow_html=True)           

            col1, col2, col3 = tab3.columns(3)

            index = 1
            for x, y in rename_cols.items():
                series = y
                if(y != 'DATE'):
                    chart_settings = {
                        "title": y, 
                        "xlabel": "Period", 
                        "ylabel": "Index", 
                        "ypercentage": False,
                    }

                    display_chart_ism(chart_settings, df_production_last_6_months, series, col=eval("col" + str(index)))

                    if(index == 3):
                        index = 1
                    else:
                        index = index + 1

            #TAB 4
            tab4.subheader(tabs[3])

            #TODO
            rename_cols = {'ism_date': 'DATE','apparel_leather_allied_products':'Apparel Leather Allied Products','chemical_products':'Chemical Products','computer_electronic_products':'Computer & Electronic Products','electrical_equipment_appliances_components':'Electrical Equipment Appliances Components','fabricated_metal_products':'Fabricated Metal Products','food_beverage_tobacco_products':'Food Beverage & Tobacco Products','furniture_related_products':'Furniture Related Products','machinery':'Machinery','miscellaneous_manufacturing':'Miscellaneous Manufacturing','nonmetallic_mineral_products':'Nonmetallic Mineral Products','paper_products':'Paper Products','petroleum_coal_products':'Petroleum Coal Products','plastics_rubber_products':'Plastics & Rubber Products','primary_metals':'Primary Metails','printing_related_support_activities':'Printing Related Support Activities','textile_mills':'Textile Mills','transportation_equipment':'Transportation Equipment','wood_products':'Wood Products'}
            df_sectors = df_sectors.rename(columns=rename_cols)

            df_sectors_trend = df_sectors.sort_values(by=['DATE'], ascending=True).T

            df_sectors_trend.columns = df_sectors_trend.iloc[0]
            df_sectors_trend = df_sectors_trend[1:]
            df_sectors_trend = df_sectors_trend.reset_index(drop=False)

            df_sectors_trend['MoM'] = df_sectors_trend.iloc[:, 6] -  df_sectors_trend.iloc[:, 5]
            df_sectors_trend['3 Months'] = df_sectors_trend[[df_sectors_trend.columns[6], df_sectors_trend.columns[5], df_sectors_trend.columns[4]]].mean(axis=1)
            df_sectors_trend['6 Months'] = df_sectors_trend[[df_sectors_trend.columns[6], df_sectors_trend.columns[5], df_sectors_trend.columns[4], df_sectors_trend.columns[3], df_sectors_trend.columns[2], df_sectors_trend.columns[1]]].mean(axis=1)
            df_sectors_trend.columns = df_sectors_trend.columns.astype(str)

            rename_cols = {'index': 'Index'}

            #TODO: Get column names dynamically so that we can apply gradient
            #cols_gradient = ['MoM','3 Months', '6 Months',df_sectors_trend.columns.astype(str)[1],df_sectors_trend.columns.astype(str)[2],df_sectors_trend.columns.astype(str)[3],df_sectors_trend.columns.astype(str)[4],df_sectors_trend.columns.astype(str)[4]]
            cols_gradient = ['MoM','3 Months', '6 Months',df_sectors_trend.columns[1],df_sectors_trend.columns[2],df_sectors_trend.columns[3],df_sectors_trend.columns[4],df_sectors_trend.columns[5],df_sectors_trend.columns[6]]

            cols_drop = []
            format_cols = {
                'MoM': '{:,.1f}'.format,
                '3 Months': '{:,.1f}'.format,
                '6 Months': '{:,.1f}'.format,
            }

            disp,df = style_df_for_display(df_sectors_trend,cols_gradient,rename_cols,cols_drop,format_cols)
            tab4.markdown(disp.to_html(), unsafe_allow_html=True)

            #TAB 5
            tab5.subheader(tabs[4])
            
            rename_cols = {'ism_date': 'Date','new_orders':'New Orders' ,'imports': 'Imports', 'backlog_of_orders':'Backlog of Orders', 'prices':'Prices', 'production':'Production', 'customers_inventories':'Customer Inventories', 'inventories':'Inventories', 'deliveries':'Deliveries', 'employment':'Employment', 'exports':'Exports', 'ism':'ISM'}

            cols_gradient = ['New Orders' , 'Imports', 'Backlog of Orders', 'Prices', 'Production','Customer Inventories','Inventories', 'Deliveries', 'Employment', 'Exports', 'ISM']
            cols_drop = []
            format_cols = {
                'Date': lambda t: t.strftime("%m-%Y")
            }
            disp,df = style_df_for_display(df_headline,cols_gradient,rename_cols,cols_drop,format_cols)
            tab5.markdown(disp.to_html(), unsafe_allow_html=True)

            #TAB 6
            tab6.subheader(tabs[5])

            #TODO

            #TAB 7
            tab7.subheader(tabs[6])

            #TODO

        if option_leading_indicator_charts == '017 - US ISM Services':    

            # gdpc1
            tab1, tab2, tab3, tab4 = st.tabs(["📈 Overall US GDP", "📈 US GDP QoQ", "📈 US GDP YoY", "📈 US GDP QoQ Annualized"])

            df_us_gdp_all, df_us_gdp_recent = get_stlouisfed_data('gdpc1', 'Q', 10)

            #TAB 1
            tab1.subheader("Overall GDP")

            series = "gdpc1"
            chart_settings = {
                "type": "line",
                "title": "Total US GDP", 
                "xlabel": "Year", 
                "ylabel": "GDP", 
                "ypercentage": False,

            }

            #display_chart(chart_settings, df_us_gdp_all, series, tab1)


        #fig, (ax_plot1, ax_plot2) = plt.subplots(
        #    nrows=2,
        #    ncols=1,
        #    figsize=(10,10)
        #)
        #ax_plot1.hist(df_us_gdp_all["gdpc1"], bins=20)
        #ax_plot2.hist(df_us_gdp_recent["gdpc1"], bins=20)

        pass

    #TODO: Learn about plotting and graphing in python - https://www.youtube.com/watch?v=6GUZXDef2U0&t=2712s
    #TODO: Learn about different tabular ways to represent data in python, including colour gradient (ie. seaborne?)
    #TODO: Learn how to create "tabs" to show different types of charts on a single page

if option == 'Single Stock One Pager':
    st.write("Get 1 page quantitative data for a Company")
    symbol = st.sidebar.text_input("Symbol", value='MSFT', max_chars=None, key=None, type='default')
    clicked = st.sidebar.button("Get One Pager")
    logger = get_logger()

    if('single_stock_one_pager_clicked' in st.session_state):
        clicked = st.session_state['single_stock_one_pager_clicked']

    if(clicked):
        if('single_stock_one_pager_clicked' not in st.session_state):
            st.session_state['single_stock_one_pager_clicked'] = True

        option_one_pager = st.sidebar.selectbox("Which Dashboard?", ('Quantitative Data','Price Action',), 0)

        if option_one_pager == 'Quantitative Data':
            #Get all the data for this stock from the database
            try:
                df_company_details, df_zacks_balance_sheet_shares, df_zacks_earnings_surprises, df_zacks_product_line_geography, df_stockrow_stock_data, df_yf_key_stats, df_zacks_peer_comparison, df_finwiz_stock_data, df_dcf_valuation = get_one_pager(symbol)
            except UnboundLocalError as e:
                st.markdown("Company Not Found")
            else:
                #json_yf_module_summaryProfile, json_yf_module_financialData,json_yf_module_summaryDetail,json_yf_module_price,json_yf_module_defaultKeyStatistics,yf_error = get_yf_price_action(symbol,logger)

                json_module_profile, json_module_quote, json_module_balance_sheet, json_module_key_metrics, json_module_company_outlook, json_module_price_target_summary,json_module_key_metrics_ttm,json_module_company_core_information, json_module_company_income_statement, error = get_financialmodelingprep_price_action(symbol,logger)

                #import pdb; pdb.set_trace()
                #if not yf_error:
                #    dataSummaryDetail = json_yf_module_summaryDetail['quoteSummary']['result'][0]['summaryDetail']                             #json_price_action['quoteSummary']['result'][0]['summaryDetail']
                #    dataDefaultKeyStatistics = json_yf_module_defaultKeyStatistics['quoteSummary']['result'][0]['defaultKeyStatistics']        #json_price_action['quoteSummary']['result'][0]['defaultKeyStatistics']
                #    dataSummaryProfile = json_yf_module_summaryProfile['quoteSummary']['result'][0]['summaryProfile']                          #json_price_action['quoteSummary']['result'][0]['summaryProfile']
                #    dataFinancialData = json_yf_module_financialData['quoteSummary']['result'][0]['financialData']                             #json_price_action['quoteSummary']['result'][0]['financialData']
                #    dataPrice = json_yf_module_price['quoteSummary']['result'][0]['price']                                                     #json_price_action['quoteSummary']['result'][0]['price']
                #else:
                #    dataSummaryDetail = {}                           
                #    dataDefaultKeyStatistics = {}      
                #    dataSummaryProfile = {}                     
                #    dataFinancialData = {}                             
                #    dataPrice = {}                                              

                # Get High Level Company Details
                company_name = df_company_details['company_name'][0]
                sector = df_company_details['sector'][0]
                industry = df_company_details['industry'][0]
                exchange = df_company_details['exchange'][0]

                if not error:
                    market_cap = json_module_profile[0]['mktCap']
                    market_cap ='{:,.0f}'.format(market_cap)   
                   
                    last = json_module_profile[0]['price']
                    last ='{:,.2f}'.format(last)

                    currency = json_module_profile[0]['currency']
                    website = json_module_profile[0]['website']
                    volume = json_module_quote[0]['volume']
                    business_summary = json_module_profile[0]['description']

                    annual_high = json_module_quote[0]['yearHigh']
                    annual_high ='{:,.2f}'.format(annual_high)                    

                    annual_low = json_module_quote[0]['yearLow']                    
                    annual_low ='{:,.2f}'.format(annual_low)                    

                    #TODO: NEED TO USE
                    earnings_announcement = json_module_quote[0]['earningsAnnouncement']
                    #import pdb; pdb.set_trace()
                    try:
                        dt_earnings_announcement = dt.strptime(earnings_announcement, '%Y-%m-%dT%H:%M:00.000+0000')
                        earnings_date_str = dt_earnings_announcement.strftime("%d %b %Y")
                    except ValueError as e:
                        earnings_date_str = "TBC"

                    total_debt = json_module_balance_sheet[0]['totalDebt'] 
                    
                    #Calculate EV
                    ev = json_module_key_metrics[0]['enterpriseValue']

                    #dividend_this_year = dataSummaryDetail['trailingAnnualDividendRate']['raw']
                    dividend_this_year = json_module_key_metrics_ttm[0]['dividendPerShareTTM']
                    dividend_this_year_formatted ='{:,.2f}'.format(dividend_this_year)

                else:
                    market_cap = None
                    market_cap = None
                    last = None
                    annual_high = None
                    annual_low = None
                    currency = None
                    website = None
                    volume = None 
                    business_summary = None
                    total_debt = None
                    ev = None
                    dividend_this_year = None
                    dividend_this_year_formatted = None
                    earnings_date_str = None

                try:
                    dcf_valuation = df_dcf_valuation['dcf'][0]
                    dcf_valuation ='{:,.2f}'.format(dcf_valuation) 
                except KeyError as e:
                    dcf_valuation = None

                try:
                    beta = json_module_profile[0]['beta']
                    beta ='{:,.3f}'.format(beta) 

                except KeyError as e:
                    beta = 0

                try:
                    div_yield = json_module_key_metrics[0]['dividendYield'] 
                except KeyError as e:
                    div_yield = None

                try:
                    #target_price = dataFinancialData['targetHighPrice']['fmt']
                    target_price = json_module_price_target_summary[0]['lastMonthAvgPriceTarget']
                    target_price ='{:,.2f}'.format(target_price)
                except (IndexError, KeyError) as e:
                    target_price = None

                try:
                    #next_fiscal_year_end = dataDefaultKeyStatistics['nextFiscalYearEnd']['fmt']
                    next_fiscal_year_end = json_module_company_core_information[0]['fiscalYearEnd']
                except KeyError as e:
                    next_fiscal_year_end = None

                try: 
                    #days_to_cover_short_ratio = dataDefaultKeyStatistics['shortRatio']['raw']
                    days_to_cover_short_ratio = json_module_company_outlook['ratios'][0]['shortTermCoverageRatiosTTM']
                    days_to_cover_short_ratio_formatted ='{:,.2f}'.format(days_to_cover_short_ratio)
                except KeyError as e:
                    days_to_cover_short_ratio_formatted = None

                #market_cap_formatted ='{:,.2f}'.format(market_cap)
                shares_outstanding = df_company_details['shares_outstanding'][0]
                ev = df_yf_key_stats['ev'][0]
 
                if(len(df_finwiz_stock_data) > 0):
                    range_52w = df_finwiz_stock_data['range_52w'][0]
                    trailing_pe = df_finwiz_stock_data['pe'][0]
                    forward_pe = df_finwiz_stock_data['pe_forward'][0]
                    peg_ratio = df_finwiz_stock_data['peg'][0]        
                    roe = df_finwiz_stock_data['roe'][0]

                else:
                    range_52w = None
                    trailing_pe = None
                    forward_pe = None
                    peg_ratio = None      
                    roe = None

                shares_outstanding_formatted = '{:,.2f}'.format(shares_outstanding).split('.00')[0]
                avg_vol_3m = df_yf_key_stats['avg_vol_3m'][0]
                avg_vol_10d = df_yf_key_stats['avg_vol_10d'][0]

                percent_change_ytd = df_tickers_all.loc[df_tickers_all['Ticker']==symbol,'% Price Change (YTD)']
                percent_change_ytd =  percent_change_ytd.values[0]
                percent_change_ytd_formatted = '{:,.2f}%'.format(percent_change_ytd)
                moving_avg_50d = df_yf_key_stats['moving_avg_50d'][0]
                moving_avg_200d = df_yf_key_stats['moving_avg_200d'][0]

                column_names = ['Next Earnings Call','Last','52 Week High','52 Week Low','YTD Change %','Market Cap', 'EV', 'Days to Cover', 'Target Price', 'DCF Valuation']
                column_data = [earnings_date_str,last, annual_high, annual_low, percent_change_ytd_formatted, market_cap, ev, days_to_cover_short_ratio_formatted, target_price,dcf_valuation]
                style_t1 = format_fields_for_dashboard(column_names, column_data)

                column_names = ['Trailing P/E','Forward P/E','PEG','Divedend Y0','Dividend Yield', 'Beta', 'Currency','ROE','Exchange','Sector','Industry','Website', 'Year End']
                column_data = [trailing_pe, forward_pe, peg_ratio, dividend_this_year_formatted, div_yield, beta, currency, roe, exchange, sector, industry, website, next_fiscal_year_end,]
                style_t2 = format_fields_for_dashboard(column_names, column_data)

                column_names = ['Average Volume 3m','Average Volume 10d', 'Moving Average 50d', 'Moving Average 200d']
                column_data = [avg_vol_3m, avg_vol_10d, moving_avg_50d, moving_avg_200d]
                style_t3 = format_fields_for_dashboard(column_names, column_data)

                #############################
                #  Start of Display of Page #
                #############################

                st.subheader(f'{company_name} ({symbol})')
                st.markdown(business_summary)

                st.markdown("""---""")

                col1,col2 = st.columns(2)

                style_t1.hide_columns()
                col1.write(style_t1.to_html(), unsafe_allow_html=True)
                
                style_t2.hide_columns()
                col2.write(style_t2.to_html(), unsafe_allow_html=True)

                style_t3.hide_columns()
                #col3.write(style_t3.to_html(), unsafe_allow_html=True)
                col1.markdown("""---""")
                col1.write(style_t3.to_html(), unsafe_allow_html=True)

                st.markdown("""---""")
                df_stockrow_stock_data = df_stockrow_stock_data.sort_values(by=['forecast_year'], ascending=True)

                #df_stockrow_stock_data['net_income'] = pd.to_numeric(df_stockrow_stock_data['net_income'])
                #df_stockrow_stock_data['sales'] = pd.to_numeric(df_stockrow_stock_data['sales'])
                #df_stockrow_stock_data['fcf'] = pd.to_numeric(df_stockrow_stock_data['fcf'])
                #df_stockrow_stock_data['total_debt'] = pd.to_numeric(df_stockrow_stock_data['total_debt'])

                rename_cols = {'sales': 'Sales','ebit': 'EBIT','net_income': 'Net Income','pe_ratio': 'PE Ratio','earnings_per_share': 'EPS','cash_flow_per_share': 'Cash Flow Per Share','book_value_per_share': 'Book Value Per Share','total_debt': 'Total Debt','ebitda': 'EBITDA', 'fcf': "FCF", 'forecast_year': 'Year'}
                cols_gradient = ['Sales', 'Net Income', 'FCF']
                cols_drop = ['id', 'cid']
                format_cols = {}
                format_cols = {
                    'Sales': '{:,.0f}'.format,
                    'EBIT': '{:,.0f}'.format,
                    'Net Income': '{:,.0f}'.format,
                    'Total Debt': '{:,.0f}'.format,
                    'EBITDA': '{:,.0f}'.format,
                    'FCF': '{:,.0f}'.format,
                }

                disp,df = style_df_for_display(df_stockrow_stock_data,cols_gradient,rename_cols,cols_drop,format_cols)
                try:
                    st.markdown(disp.to_html(), unsafe_allow_html=True)           
                except TypeError as e:
                    st.dataframe(df_stockrow_stock_data, use_container_width=True)

                st.markdown("""---""")

                col1,col2 = st.columns(2)

                # Display Chart
                x_axis = 'forecast_year'
                y_axis = 'sales'
                chart_settings = {
                    "type": "bar",
                    "title": "Sales", 
                    "xlabel": "Year", 
                    "ylabel": "Sales", 
                    "ypercentage": False,
                }

                display_chart_assets(chart_settings, df_stockrow_stock_data, x_axis, y_axis, col1)

                # Display Chart
                x_axis = 'forecast_year'
                y_axis = 'net_income'
                chart_settings = {
                    "type": "bar",
                    "title": "Net Income", 
                    "xlabel": "Year", 
                    "ylabel": "Net Income", 
                    "ypercentage": False,
                }

                display_chart_assets(chart_settings, df_stockrow_stock_data, x_axis, y_axis, col2)

                # Display Chart
                x_axis = 'forecast_year'
                y_axis = 'fcf'
                chart_settings = {
                    "type": "bar",
                    "title": "Free Cash Flow", 
                    "xlabel": "Year", 
                    "ylabel": "FCF", 
                    "ypercentage": False,
                }

                display_chart_assets(chart_settings, df_stockrow_stock_data, x_axis, y_axis, col1)

                # Display Chart
                x_axis = 'forecast_year'
                y_axis = 'total_debt'
                chart_settings = {
                    "type": "bar",
                    "title": "Tobal Debt", 
                    "xlabel": "Year", 
                    "ylabel": "Debt", 
                    "ypercentage": False,
                }

                display_chart_assets(chart_settings, df_stockrow_stock_data, x_axis, y_axis, col2)

                st.markdown("""---""")

                st.markdown("Earnings Surprises")
                #import pdb; pdb.set_trace()
                df_zacks_earnings_surprises = df_zacks_earnings_surprises.sort_values(by=['dt'], ascending=True)

                # get a list of columns
                cols = list(df_zacks_earnings_surprises)

                cols.insert(0, cols.pop(cols.index('id')))
                cols.insert(1, cols.pop(cols.index('cid')))
                cols.insert(2, cols.pop(cols.index('dt')))
                cols.insert(3, cols.pop(cols.index('reporting_period')))
                cols.insert(4, cols.pop(cols.index('sales_estimate')))
                cols.insert(5, cols.pop(cols.index('sales_reported')))
                cols.insert(6, cols.pop(cols.index('eps_estimate')))
                cols.insert(7, cols.pop(cols.index('eps_reported')))

                # reorder
                df_zacks_earnings_surprises = df_zacks_earnings_surprises[cols]

                rename_cols = {'reporting_period': 'Period','eps_estimate': 'EPS Estimate','eps_reported': 'EPS Actual','sales_estimate': 'Sales Estimate','sales_reported': 'Sales Actual'}
                cols_gradient = []
                cols_drop = ['id', 'cid','dt']
                format_cols = {
                    'Sales Estimate': '{:,.2f}'.format,
                    'Sales Actual': '{:,.0f}'.format,
                }

                disp,df = style_df_for_display(df_zacks_earnings_surprises,cols_gradient,rename_cols,cols_drop,format_cols)
                #st.markdown(disp.to_html(), unsafe_allow_html=True)
                df_style = disp.apply(format_earnings_surprises, subset=['EPS Estimate', 'EPS Actual'], axis=1)
                df_style = disp.apply(format_earnings_surprises, subset=['Sales Estimate', 'Sales Actual'], axis=1)
                st.markdown(df_style.to_html(), unsafe_allow_html=True)

                st.markdown("""---""")

                col1,col2 = st.columns(2)

                col1.markdown("Geography") 

                if(len(df_zacks_product_line_geography) > 0):

                    df_zacks_product_line_geography['revenue'] = pd.to_numeric(df_zacks_product_line_geography['revenue'])
                    df_zacks_product_line_geography = df_zacks_product_line_geography.sort_values(by=['revenue'], ascending=False)

                    rename_cols = {'region':'Region','revenue':'Revenue'}
                    cols_gradient = []
                    cols_drop = ['id', 'cid']
                    format_cols = {
                        'Revenue': '{:,.0f}'.format,
                    }

                    disp,df = style_df_for_display(df_zacks_product_line_geography,cols_gradient,rename_cols,cols_drop,format_cols)
                    col1.markdown(disp.to_html(), unsafe_allow_html=True)
                else:
                    col1.markdown("Geography data does not exist")

                col2.markdown("Peers")
                df_zacks_peer_comparison = df_zacks_peer_comparison.sort_values(by=['peer_ticker'], ascending=True)
                rename_cols = {'peer_company': 'Peer Company','peer_ticker': 'Peer Ticker'}
                cols_gradient = []
                cols_drop = ['id', 'cid']
                format_cols = {}

                disp,df = style_df_for_display(df_zacks_peer_comparison,cols_gradient,rename_cols,cols_drop,format_cols)
                col2.markdown(disp.to_html(), unsafe_allow_html=True)


                #sort_cols = ['peer_ticker']
                #drop_cols = ['cid','id' ]
                #rename_cols = {'peer_company': 'Peer Company','peer_ticker': 'Peer Ticker'}
                #format_cols = []

                #style_t7 = format_df_for_dashboard(df_zacks_peer_comparison, sort_cols, drop_cols, rename_cols, format_cols=format_cols)
                #col2.write(style_t7) ##TODO: CHANGE FORMATTING OF TABLE

                st.markdown("""---""")

                df_zacks_peer_comparison = df_zacks_peer_comparison.rename(columns={"peer_ticker":"Ticker"})
                st.markdown("Peer Comparison")
                df_peers = get_summary_ratios(df_zacks_peer_comparison)

                df_peers = df_peers.T
                new_header = df_peers.iloc[0] #grab the first row for the header
                df_peers = df_peers[1:] #take the data less the header row

                df_peers.columns = new_header #set the header row as the df header

                #import pdb; pdb.set_trace()
                df_peers = df_peers.reset_index(drop=False)
                #TODO: Get peer details and display it in a table
                #df_peers = get_peer_details(df_zacks_peer_comparison, logger)
                rename_cols = {}
                cols_gradient = []
                cols_drop = []
                format_cols = {}

                disp,df = style_df_for_display(df_peers,cols_gradient,rename_cols,cols_drop,format_cols)
                st.markdown(disp.to_html(), unsafe_allow_html=True)

                #st.dataframe(df_peers) ##TODO: CHANGE FORMATTING OF TABLE

        if option_one_pager == 'Price Action':
            st.subheader(f'EMA Entries For: {symbol}')
            fig1 = plot_ticker_signals_ema(symbol,logger)
            st.plotly_chart(fig1)

            st.markdown("""---""")

            st.subheader(f'VWAP Entries For: {symbol}')
            fig2 = plot_ticker_signals_vwap(symbol,logger)
            st.plotly_chart(fig2)

            st.markdown("""---""")

            st.subheader(f'Support and Resistance Levels For: {symbol}')
            fig3, plt = plot_ticker_signals_histogram(symbol, logger)
            st.plotly_chart(fig3)
            st.pyplot(plt)

            #st.image(f'https://finviz.com/chart.ashx?t={symbol}&ty=c&ta=1&p=d&s=l')

        #if option_one_pager == 'Stock Twits':

        #    st.subheader(f'Stock Twit News For: {symbol}')

        #    r = requests.get(f"https://api.stocktwits.com/api/2/streams/symbol/{symbol}.json")

        #    data = r.json()

        #    for message in data['messages']:
        #        st.image(message['user']['avatar_url'])
        #        st.write(message['user']['username'])
        #        st.write(message['created_at'])
        #        st.write(message['body'])

if option == 'ATR Calculator':

    symbol1 = st.sidebar.text_input("Symbol 1", value='MSFT', max_chars=None, key=None, type='default')
    symbol2 = st.sidebar.text_input("Symbol 2", value='CRM', max_chars=None, key=None, type='default')

    clicked = st.sidebar.button("Get ATR")

    if(clicked):
        if(len(symbol1.strip()) > 0 and len(symbol2.strip()) > 0):        
            #TODO: Need to add formats for date and %
            sort_cols = []
            drop_cols = ['Open','High','Low','Close','H-L','H-C','L-C','TR','ATR']
            rename_cols = {}
            format_cols = {'DATE': 'date','ATR %': 'percentage' }

            index1 = symbol1.strip()
            df_symbol1_sorted_daily_atr, df_symbol1_sorted_monthly_atr, df_symbol1_sorted_quarterly_atr, df_symbol1_sorted_daily_price = get_atr_prices(index1, 1)
            df_index1 = df_symbol1_sorted_daily_price.drop(['Open', 'High', 'Low','ATR %'], axis=1)

            col1,col2,col3 = st.columns(3)

            col1.markdown(f"{index1} Daily ATR") 
            col2.markdown(f"{index1} Monthly ATR") 
            col3.markdown(f"{index1} Quarterly ATR") 

            style_t1 = format_df_for_dashboard(df_symbol1_sorted_daily_atr, sort_cols, drop_cols, rename_cols, format_cols=format_cols)
            col1.write(style_t1)

            style_t2 = format_df_for_dashboard(df_symbol1_sorted_monthly_atr, sort_cols, drop_cols, rename_cols, format_cols=format_cols)
            col2.write(style_t2)

            style_t3 = format_df_for_dashboard(df_symbol1_sorted_quarterly_atr, sort_cols, drop_cols, rename_cols, format_cols=format_cols)
            col3.write(style_t3)

            index2 = symbol2.strip()
            df_symbol2_sorted_daily_atr, df_symbol2_sorted_monthly_atr, df_symbol2_sorted_quarterly_atr, df_symbol2_sorted_daily_price = get_atr_prices(index2, 2)
            df_index2 = df_symbol2_sorted_daily_price.drop(['Open', 'High', 'Low','ATR %'], axis=1)

            col1.markdown(f"{index2} Daily ATR") 
            col2.markdown(f"{index2} Monthly ATR") 
            col3.markdown(f"{index2} Quarterly ATR") 

            style_t4 = format_df_for_dashboard(df_symbol2_sorted_daily_atr, sort_cols, drop_cols, rename_cols, format_cols=format_cols)
            col1.write(style_t4)

            style_t5 = format_df_for_dashboard(df_symbol2_sorted_monthly_atr, sort_cols, drop_cols, rename_cols, format_cols=format_cols)
            col2.write(style_t5)

            style_t6 = format_df_for_dashboard(df_symbol2_sorted_quarterly_atr, sort_cols, drop_cols, rename_cols, format_cols=format_cols)
            col3.write(style_t6)

            st.markdown("""---""")

            df_updated_indexes = combine_df_on_index(df_index1, df_index2, 'DATE')

            df_sorted = df_updated_indexes.sort_values(by='DATE', ascending = False)

            # Sort by putting symbol1 first and symbol2 second. 
            df_ordered = df_sorted.loc[:, ['DATE',symbol1,symbol2]]

            # Format Date Field
            df_ordered['DATE'] = df_ordered['DATE'].dt.strftime('%d-%m-%Y')
            file_date = dt.now().strftime('%Y%m%d_%H%M%S')

            filename = f'ATR_{symbol1}_{symbol2}_{file_date}.xlsx'

            #TODO: Write to excel file with multiple tabs - Price Action, ATR Daily, ATR Monthly, ATR Quarterly
            df_xlsx = atr_to_excel(df_ordered,
                               df_symbol1_sorted_daily_atr,
                               df_symbol1_sorted_monthly_atr,
                               df_symbol1_sorted_quarterly_atr,
                               df_symbol2_sorted_daily_atr,
                               df_symbol2_sorted_monthly_atr,
                               df_symbol2_sorted_quarterly_atr                               
                               )
            st.download_button(label='📥 Download ATR Results',
                                            data=df_xlsx ,
                                            file_name=filename)
        else:
            st.write("Please enter 2 ticker symbols")

if option == 'Bottom Up Ideas':
        option_one_pager = st.sidebar.selectbox("Which Dashboard?", ('Volume','TA Patterns', 'DCF Stock Valuation','Country Exposure'), 0)
        if option_one_pager == 'Volume':        
            df_stock_volume = sql_get_volume()

            col1,col2 = st.columns(2)

            df_vol_data_all_sectors = df_stock_volume.drop(['cid','id','industry','vs_avg_vol_10d','vs_avg_vol_3m', 'outlook', 'company_name', 'percentage_sold', 'last_close', 'symbol'], axis=1)
            df_vol_data_all_sectors = df_vol_data_all_sectors.groupby(['sector']).sum().sort_values(by=['last_volume'], ascending=False).reset_index()

            df_vol_data_all_sectors = df_vol_data_all_sectors.head(5)
            df_vol_data_all_sectors = format_volume_df(df_vol_data_all_sectors)

            sort_cols = []
            drop_cols = []
            rename_cols = {'sector': 'Sector','last_volume': 'Volume'}
            number_format_cols = []

            col1.subheader(f'Volume by Sectors (Last 24 Hrs)')

            # Display Chart
            x_axis = 'sector'
            y_axis = 'last_volume'
            chart_settings = {
                "type": "bar",
                "title": "Volume - Sector", 
                "xlabel": "Sectors", 
                "ylabel": "Volume", 
                "ypercentage": False,
            }

            display_chart_assets(chart_settings, df_vol_data_all_sectors, x_axis, y_axis, col1)

            df_vol_data_all_industries = df_stock_volume.drop(['cid','id','sector','vs_avg_vol_10d','vs_avg_vol_3m', 'outlook', 'company_name', 'percentage_sold', 'last_close', 'symbol'], axis=1)
            df_vol_data_all_industries = df_vol_data_all_industries.groupby(['industry']).sum().sort_values(by=['last_volume'], ascending=False).reset_index()

            df_vol_data_all_industries = df_vol_data_all_industries.head(10)
            df_vol_data_all_industries = format_volume_df(df_vol_data_all_industries)

            sort_cols = []
            drop_cols = []
            rename_cols = {'industry': 'Industry','last_volume': 'Volume'}
            number_format_cols = []

            col2.subheader(f'Volume by Industries (Last 24 Hrs)')

            # Display Chart
            x_axis = 'industry'
            y_axis = 'last_volume'
            chart_settings = {
                "type": "bar",
                "title": "Volume - Industry", 
                "xlabel": "Industries", 
                "ylabel": "Volume", 
                "ypercentage": False,
            }

            display_chart_assets(chart_settings, df_vol_data_all_industries, x_axis, y_axis, col2)

            if(len(df_stock_volume) > 0):
                st.markdown("""---""")

                st.subheader(f'Volume by Individual Stocks')
                st.markdown(f'High Volume Vs Last 3 Months')
                df_stock_volume_3m = df_stock_volume.sort_values(by=['vs_avg_vol_3m'], ascending=False)        
                df_stock_volume_3m = df_stock_volume_3m[df_stock_volume['vs_avg_vol_3m'] > 4].reset_index()
                df_stock_volume_3m = format_volume_df(df_stock_volume_3m)    
                
                #Display formatted table
                format_cols = {}
                cols_gradient = []
                rename_cols = {'vs_avg_vol_10d': '% Avg Vol 10d', 'vs_avg_vol_3m': '% Avg Vol 3m', 'outlook': 'Outlook', 'symbol': 'Symbol', 'last_close': 'Last', 'company_name': 'Company', 'sector': 'Sector', 'industry': 'Industry', 'percentage_sold': '% Sold'}
                drop_cols = ['index','id', 'cid','last_volume']

                disp, df_display_table = style_df_for_display(df_stock_volume_3m,cols_gradient,rename_cols,drop_cols,cols_format=format_cols,format_rows=False)
                df_style = disp.apply(format_bullish_bearish, subset=['Outlook'], axis=1)

                st.markdown(df_style.to_html(), unsafe_allow_html=True)   

                st.markdown("""---""")

                st.markdown(f'High Volume Last 24h')
                df_stock_volume_1d = df_stock_volume.sort_values(by=['percentage_sold'], ascending=False)        
                df_stock_volume_1d = df_stock_volume_1d[df_stock_volume['percentage_sold'] > 0.05].reset_index()
                df_stock_volume_1d = format_volume_df(df_stock_volume_1d)
                #import pdb; pdb.set_trace()
                df = df_stock_volume_1d.reset_index(drop=True)
                data = {'symbol':[],'company_name':[],'sector':[],'industry':[],'percentage_sold':[],'outlook':[]}
                for index, row in df.iterrows():
                    symbol = row['symbol']
                    df_company_row = pd.DataFrame(data)
                    temp_row = [row['symbol'],row['company_name'],row['sector'],row['industry'],row['percentage_sold'],row['outlook']]
    
                    df_company_row.loc[len(df.index)] = temp_row
                    # Format each DF before printing

                    #Display formatted table
                    format_cols = {}
                    cols_gradient = []
                    rename_cols = {'symbol': 'Symbol', 'company_name': 'Company', 'sector': 'Sector', 'industry': 'Industry', 'percentage_sold': '% Traded Today', 'outlook': 'Outlook'}
                    drop_cols = []

                    disp, df_display_table = style_df_for_display(df_company_row,cols_gradient,rename_cols,drop_cols,cols_format=format_cols,format_rows=False)
                    df_style = disp.apply(format_bullish_bearish, subset=['Outlook'], axis=1)
                    st.markdown(df_style.to_html(), unsafe_allow_html=True) 

                    st.image(f'https://finviz.com/chart.ashx?t={symbol}&ty=c&ta=1&p=d&s=l')
                    st.markdown("""---""")
            else:
                st.markdown("No Stock Volume Data Available")

            #st.markdown("Price Action Volume")
            #st.dataframe(df)

        if option_one_pager == 'TA Patterns':
            st.subheader(f'TA Patterns')
            df = get_data(table="ta_patterns")
            df_tickers = get_data(table="company") 
            df_inner_join = pd.merge(df, df_tickers, left_on='ticker', right_on='symbol', how='inner')

            df_consolidating = df_inner_join.loc[df_inner_join['pattern'] == 'consolidating']            
            df_breakout = df_inner_join.loc[df_inner_join['pattern'] == 'breakout']
            df_breaking_sma_50_150_last_14_days = df_inner_join.loc[df_inner_join['pattern'] == 'sma_breakout_50_150_14']


            tab1, tab2, tab3 = st.tabs(["📈 Consolidating", "📈 Breakout", "SMA 50-150 Breakout"])

            #TAB 1
            tab1.subheader("Consolidating")

            data = {'ticker':[],'company_name':[],'sector':[],'industry':[],'pattern':[]}

            for index, row in df_consolidating.iterrows():
                symbol = row['ticker']
                df_temp = pd.DataFrame(data)
                temp_row = [row['ticker'],row['company_name'],row['sector'],row['industry'],row['pattern']]
                df_temp.loc[len(df.index)] = temp_row

                #Display formatted table
                format_cols = {}
                cols_gradient = []
                rename_cols = {'ticker': 'Ticker', 'pattern': 'Pattern','company_name':'Company','sector':'Sector','industry':'Industry'}
                drop_cols = []
                disp, df_display_table = style_df_for_display(df_temp,cols_gradient,rename_cols,drop_cols,cols_format=format_cols,format_rows=False)
                tab1.markdown(disp.to_html(), unsafe_allow_html=True)   
                tab1.image(f'https://finviz.com/chart.ashx?t={symbol}&ty=c&ta=1&p=d&s=l')
                tab1.markdown("""---""")

            tab2.subheader("Breakout")
            data = {'ticker':[],'company_name':[],'sector':[],'industry':[],'pattern':[]}
            for index, row in df_breakout.iterrows():
                symbol = row['ticker']
                df_temp = pd.DataFrame(data)
                temp_row = [row['ticker'],row['company_name'],row['sector'],row['industry'],row['pattern']]
                df_temp.loc[len(df.index)] = temp_row

                #Display formatted table
                format_cols = {}
                cols_gradient = []
                rename_cols = {'ticker': 'Ticker', 'pattern': 'Pattern','company_name':'Company','sector':'Sector','industry':'Industry'}
                drop_cols = []
                disp, df_display_table = style_df_for_display(df_temp,cols_gradient,rename_cols,drop_cols,cols_format=format_cols,format_rows=False)
                tab2.markdown(disp.to_html(), unsafe_allow_html=True)   
                tab2.image(f'https://finviz.com/chart.ashx?t={symbol}&ty=c&ta=1&p=d&s=l')
                tab2.markdown("""---""")

            tab3.subheader("SMA 50-150 Breakout (Past 14 Days)")
            data = {'ticker':[],'company_name':[],'sector':[],'industry':[],'pattern':[]}
            for index, row in df_breaking_sma_50_150_last_14_days.iterrows():
                symbol = row['ticker']
                df_temp = pd.DataFrame(data)
                temp_row = [row['ticker'],row['company_name'],row['sector'],row['industry'],row['pattern']]
                df_temp.loc[len(df.index)] = temp_row

                #Display formatted table
                format_cols = {}
                cols_gradient = []
                rename_cols = {'ticker': 'Ticker', 'pattern': 'Pattern','company_name':'Company','sector':'Sector','industry':'Industry'}
                drop_cols = []
                disp, df_display_table = style_df_for_display(df_temp,cols_gradient,rename_cols,drop_cols,cols_format=format_cols,format_rows=False)
                tab3.markdown(disp.to_html(), unsafe_allow_html=True)   
                tab3.image(f'https://finviz.com/chart.ashx?t={symbol}&ty=c&ta=1&p=d&s=l')
                tab3.markdown("""---""")


        #if option_one_pager == 'Insider Trading':        
        #    st.subheader(f'Insider Trading')
        #    df = get_data(table="macro_insidertrading")

        #    sort_cols = []
        #    order_cols = ['filing_date','company_ticker','company_name', 'insider_name', 'insider_title', 'trade_type', 'trade_price', 'percentage_owned']
        #    drop_cols = ['id']
        #    rename_cols = {'filing_date': 'Filing Date', 'company_ticker': 'Ticker', 'company_name': 'Company', 'insider_name': 'Insider', 'insider_title': 'Title', 'trade_type': 'Trade', 'trade_price': 'Price', 'percentage_owned': '% Owned'}

        #    style_insider_trading = format_df_for_dashboard(df, sort_cols, drop_cols, rename_cols, order_cols=order_cols)                

        #    st.write(style_insider_trading)

        if option_one_pager == 'DCF Stock Valuation':

            #TODO: Sort results by SECTOR so that I know which sectors are overvalued-undervalued. Or create a new tab for summary.
            st.subheader(f'DCF Stock Valuation')
            df_dcf = get_data(table="companystockvaluedcf")
            df_dcf = df_dcf.rename(columns={"dt": "DATE"})
            df_tickers = get_data(table="company") 
            df_inner_join = pd.merge(df_dcf, df_tickers, left_on='cid', right_on='cid', how='inner')
            df_inner_join = df_inner_join.drop(['exchange', 'shares_outstanding'], axis=1)
            df_moderate_undervalued = df_inner_join.loc[df_inner_join['under_over'] == 'moderate undervalued'] #.sort_values(by=['sector'], ascending=True)         
            df_grossly_undervalued = df_inner_join.loc[df_inner_join['under_over'] == 'grossly undervalued'] #.sort_values(by=['sector'], ascending=True) 
            df_moderate_overvalued = df_inner_join.loc[df_inner_join['under_over'] == 'moderate overvalued'] #.sort_values(by=['sector'], ascending=True) 
            df_grossly_overvalued = df_inner_join.loc[df_inner_join['under_over'] == 'grossly overvalued'] #.sort_values(by=['sector'], ascending=True) 

            tab1, tab2, tab3, tab4, tab5 = st.tabs(["📈 Summary","📈 Grossly Undervalued", "📈 Moderately Undervalued", "📈 Moderately Overvalued", "📈 Grossly Overvalued"])

            # TAB 1
            col1, col2, col3, col4 = tab1.columns(4)

            df_dcf_group_grossly_undervalued = df_grossly_undervalued.groupby(['sector']).count()['under_over']
            df_dcf_group_moderate_undervalued = df_moderate_undervalued.groupby(['sector']).count()['under_over']
            df_dcf_group_moderate_overvalued = df_moderate_overvalued.groupby(['sector']).count()['under_over']
            df_dcf_group_grossly_overvalued = df_grossly_overvalued.groupby(['sector']).count()['under_over']

            df_dcf_group_grossly_undervalued = df_dcf_group_grossly_undervalued.to_frame().reset_index().sort_values(by=['under_over'], ascending=False)
            df_dcf_group_moderate_undervalued = df_dcf_group_moderate_undervalued.to_frame().reset_index().sort_values(by=['under_over'], ascending=False)
            df_dcf_group_moderate_overvalued = df_dcf_group_moderate_overvalued.to_frame().reset_index().sort_values(by=['under_over'], ascending=False)
            df_dcf_group_grossly_overvalued = df_dcf_group_grossly_overvalued.to_frame().reset_index().sort_values(by=['under_over'], ascending=False)

            rename_cols = {'sector':'Sector', 'under_over': 'Count'}
            cols_gradient = []
            cols_drop = []
            format_cols = {}

            col1.markdown("Grossly Undervalued")
            disp,df = style_df_for_display(df_dcf_group_grossly_undervalued,cols_gradient,rename_cols,cols_drop,format_cols)
            col1.markdown(disp.to_html(), unsafe_allow_html=True)

            col2.markdown("Moderately Undervalued")
            disp,df = style_df_for_display(df_dcf_group_moderate_undervalued,cols_gradient,rename_cols,cols_drop,format_cols)
            col2.markdown(disp.to_html(), unsafe_allow_html=True)

            col3.markdown("Moderately Overvalued")
            disp,df = style_df_for_display(df_dcf_group_moderate_overvalued,cols_gradient,rename_cols,cols_drop,format_cols)
            col3.markdown(disp.to_html(), unsafe_allow_html=True)

            col4.markdown("Grossly Overvalued")
            disp,df = style_df_for_display(df_dcf_group_grossly_overvalued,cols_gradient,rename_cols,cols_drop,format_cols)
            col4.markdown(disp.to_html(), unsafe_allow_html=True)
            #import pdb; pdb.set_trace()
            # TAB 2
            df_grossly_undervalued_sorted = df_grossly_undervalued.sort_values(by=['market_cap'], ascending=False).copy()
            rename_cols = {'DATE': 'Date', 'company_name': 'Company', 'stock_price': 'Stock Price', 'dcf': 'DCF Valuation','symbol':'Ticker', 'sector':'Sector','industry':'Industry', 'market_cap': 'Market Cap (mil)'}
            cols_gradient = []
            cols_drop = ['id','cid','under_over']
            format_cols = {
                'DCF Valuation': '{:,.2f}'.format,
                'Stock Price': '{:,.2f}'.format,
                'Date': lambda t: t.strftime("%d-%m-%Y"),
                'Market Cap (mil)': '{:,.2f}'.format,
            }
            format_date = True

            disp,df = style_df_for_display(df_grossly_undervalued_sorted,cols_gradient,rename_cols,cols_drop,format_cols)
            tab2.markdown(disp.to_html(), unsafe_allow_html=True)

            # TAB 3
            df_moderate_undervalued_sorted = df_moderate_undervalued.sort_values(by=['market_cap'], ascending=False).copy()
            disp,df = style_df_for_display(df_moderate_undervalued_sorted,cols_gradient,rename_cols,cols_drop,format_cols)
            tab3.markdown(disp.to_html(), unsafe_allow_html=True)

            # TAB 4
            df_moderate_overvalued_sorted = df_moderate_overvalued.sort_values(by=['market_cap'], ascending=False).copy()
            disp,df = style_df_for_display(df_moderate_overvalued_sorted,cols_gradient,rename_cols,cols_drop,format_cols)
            tab4.markdown(disp.to_html(), unsafe_allow_html=True)

            # TAB 5
            df_grossly_overvalued_sorted = df_grossly_overvalued.sort_values(by=['market_cap'], ascending=False).copy()
            disp,df = style_df_for_display(df_grossly_overvalued_sorted,cols_gradient,rename_cols,cols_drop,format_cols)
            tab5.markdown(disp.to_html(), unsafe_allow_html=True)

        if option_one_pager == 'Country Exposure':
            st.subheader(f'Country Exposure')
            df_geography = get_data(table="companygeography")
            df_regions = df_geography.groupby(['region']).count().reset_index()['region']

            st.markdown("Regions")
            options_amer = st.multiselect(
                'Americas',
                config.REGIONS_AMERICAS,
                [])

            options_apac = st.multiselect(
                'Asia Pacific',
                config.REGIONS_ASIA_PACIFIC,
                [])

            options_emea = st.multiselect(
                'Middle East & Africa',
                config.REGIONS_EUROPE_MIDDLE_EAST_AFRICA,
                [])

            options_selected = options_amer + options_apac + options_emea

            if(len(options_selected) > 0):
                st.write('Options Selected: ', options_selected)
                
                #TODO: Select companies where geography matches selection
                #df_geography[df_geography['region'].isin([3, 6])]
                #df_geography[df_geography.region.str.contains('oo', regex= True, na=False)]
                # Using for loop
                df_results = pd.DataFrame()
                for i in options_selected:
                    #TODO: Adjust regex to take into account caps lock and illepsis
                    df_results = df_results.append(df_geography[df_geography.region.str.contains(i, regex= True, na=False)], ignore_index=True)

                list_cids = df_results.cid.unique().tolist()
                st.write('Results: ', len(list_cids))
                #TODO: Replace with actual company tickers and names
                st.write(list_cids)
            else:
                st.write("No Selections")


        #if option_one_pager == 'Twitter':        
        #    st.subheader(f'Twitter')

        #    for username in config.TWITTER_USERNAMES:
        #        st.subheader(username)
        #        user = api.get_user(screen_name=username)
        #        tweets = api.user_timeline(screen_name=username)
        #        st.image(user.profile_image_url)
        #        for tweet in tweets:
        #            if('$' in tweet.text):
        #                words = tweet.text.split(' ')
        #                for word in words:
        #                    if word.startswith('$') and word[1:].isalpha():
        #                        symbol = word[1:]
        #                        st.write(symbol)
        #                        st.write(tweet.text)

        #                        st.image(f'https://finviz.com/chart.ashx?t={symbol}&ty=c&ta=1&p=d&s=l')
if option=='Trading Report':
    clicked1 = st.markdown("Performance & Reports > Realized Summary > Year to Date")
    clicked1 = st.button(label="Import Report",key="import_report_data")
    if(clicked1):

        df_imported_report_data = import_report_data()

        st.write(f'Successfully Imported')
