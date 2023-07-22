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
from common import set_finwiz_stock_data, set_stockrow_stock_data, set_zacks_balance_sheet_shares
from common import set_zacks_peer_comparison, set_zacks_earnings_surprises, set_zacks_product_line_geography
from common import set_yf_key_stats, get_zacks_us_companies, handle_exceptions_print_result
from common import write_zacks_ticker_data_to_db, get_logger, get_one_pager,atr_to_excel
from common import set_earningswhispers_earnings_calendar, get_atr_prices, get_stlouisfed_data
from common import set_marketscreener_economic_calendar, get_peer_details, dataframe_convert_to_numeric
from common import set_whitehouse_news, set_geopolitical_calendar, get_data, sql_get_volume
from common import set_price_action_ta, set_todays_insider_trades, combine_df_on_index
from common import style_df_for_display, format_fields_for_dashboard, get_yf_price_action
from common import format_df_for_dashboard_flip, format_df_for_dashboard, format_volume_df, format_outlook
from common import set_stlouisfed_data, temp_load_excel_data_to_db, set_ism_manufacturing, set_ism_services
from common import display_chart, return_styled_ism_table1
import seaborn as sns

debug = False
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

option = st.sidebar.selectbox("Which Option?", ('Download Data','Macroeconomic Data','Calendar', 'Single Stock One Pager','ATR Calculator', 'Bottom Up Ideas'), 2)

st.header(option)

if option == 'Download Data':

    #num_days = st.sidebar.slider('Number of days', 1, 30, 3)
    clicked1 = st.markdown("Download Price Action Data (takes 15 minutes)")
    clicked1 = st.button(label="Click to Download Price Action Data",key="price_data")

    clicked2 = st.markdown("Download Stock Data (takes 6 hours)")
    clicked2 = st.button(label="Click to Download Stock Data", key="stock_data")

    clicked3 = st.markdown("Download Macroeconomic Data (takes 45 minutes)")
    clicked3 = st.button(label="Click to Download Macroeconomic Data", key="macro_data")

    if(clicked1):
        #Download Macro Data
        logger = get_logger()
        now_start = dt.now()
        start_time = now_start.strftime("%H:%M:%S")    

        st.write(f'{start_time} - Downloading Price Data...')
        df_tickers_all = get_zacks_us_companies()       
        df_tickers = get_data(table="company") 
        with concurrent.futures.ProcessPoolExecutor() as executor:
            e1p1 = executor.submit(set_earningswhispers_earnings_calendar, df_tickers_all, logger)
            e1p2 = executor.submit(set_marketscreener_economic_calendar, logger)
            e1p3 = executor.submit(set_whitehouse_news, logger)
            e1p4 = executor.submit(set_geopolitical_calendar, logger)
            e1p5 = executor.submit(set_price_action_ta, df_tickers, logger)
            e1p6 = executor.submit(set_todays_insider_trades,logger)

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
        handle_exceptions_print_result(e1p6, 1, 6, logger)


    if(clicked2):
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

            #Executor 8: set_insider_trades_company
            #e8p1 = executor.submit(set_insider_trades_company, df_tickers1, logger)
            #e8p2 = executor.submit(set_insider_trades_company, df_tickers2, logger)
            #e8p3 = executor.submit(set_insider_trades_company, df_tickers3, logger)
            #e8p4 = executor.submit(set_insider_trades_company, df_tickers4, logger)
            #e8p5 = executor.submit(set_insider_trades_company, df_tickers5, logger)

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

        #handle_exceptions_print_result(e8p1, 8, 1, logger)
        #handle_exceptions_print_result(e8p2, 8, 2, logger)
        #handle_exceptions_print_result(e8p3, 8, 3, logger)
        #handle_exceptions_print_result(e8p4, 8, 4, logger)
        #handle_exceptions_print_result(e8p5, 8, 5, logger)

        st.write(f'Status of Finwiz Stock Data: {finwiz_stock_data_status}')

    if(clicked3):
        logger = get_logger()
        now_start = dt.now()
        start_time = now_start.strftime("%H:%M:%S")    

        st.write(f'{start_time} - Downloading Macroeconomic Data...')

        # Update all St Louis FED Data
        success = set_stlouisfed_data(config.STLOUISFED_SERIES, logger)

        # Update ISM Manufacturing
        #success = set_ism_manufacturing(logger)

        # Update ISM Services
        #success = set_ism_services(logger)
        with concurrent.futures.ProcessPoolExecutor() as executor:
            e1p1 = executor.submit(set_stlouisfed_data, config.STLOUISFED_SERIES, logger)
            e1p2 = executor.submit(set_ism_manufacturing, logger)
            e1p3 = executor.submit(set_ism_services, logger)

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

if option == 'Calendar':
    #st.subheader(f'Calendar')
    df1 = get_data(table="macro_earningscalendar")
    st.markdown("Earnings Calendar")

    sort_cols = ['dt']
    drop_cols = ['id' ]
    rename_cols = {'dt': 'Date','dt_time': 'Time', 'ticker':'Ticker', 'company_name':'Company Name', 'market_cap_mil':'Market Cap (M)'}
    #number_format_cols = ['market_cap_mil']
    format_cols = {'market_cap_mil': 'number', 'dt': 'date' }

    style_t1 = format_df_for_dashboard(df1, sort_cols, drop_cols, rename_cols, format_cols=format_cols)
    st.write(style_t1)

    df2 = get_data(table="macro_economiccalendar")
    st.markdown("Economic Calendar")

    sort_cols = ['dt']
    drop_cols = ['id']
    rename_cols = {'dt': 'Date','dt_time': 'Time', 'country':'Country', 'economic_event':'Economic Event', 'previous':'Previous Data'}
    #number_format_cols = []
    format_cols = {'dt': 'date' }
    #import pdb; pdb.set_trace()
    if(len(df2) > 0):
        style_t2 = format_df_for_dashboard(df2, sort_cols, drop_cols, rename_cols,format_cols=format_cols)
        st.write(style_t2)
    else:
        st.write("Could not retrieve economic calendar")

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

if option == 'Macroeconomic Data':
    #st.subheader(f'Macro Economic Data')
    option_indicator_type = st.sidebar.selectbox("Indicator Type", ('Lagging Indicator','Interest Rates/FX','Leading Indicator'), 0)
    logger = get_logger()

    if option_indicator_type == 'Lagging Indicator':
        st.subheader(f'Lagging Indicators')

        option_lagging_indicator_charts = st.sidebar.selectbox("Charts", ('002 - US GDP','005 - US Job Market','006 - PCE','007 - US Inflation','009 - US Industrial Production','011 - US Durable Goods', '011 - US Retail Sales'), 0)
        if option_lagging_indicator_charts == '002 - US GDP':    
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
            }

            display_chart(chart_settings, df_us_gdp_all, series, tab1)

            chart_settings = {
                "type": "line",
                "title": "Total US GDP - Last 10 Years", 
                "xlabel": "Year", 
                "ylabel": "GDP", 
            }

            display_chart(chart_settings, df_us_gdp_recent, series, tab1)
            
            rename_cols = {'DATE': 'Date', 'gdpc1': 'GDP'}
            cols_gradient = ['GDP']
            cols_drop = ['QoQ','YoY','QoQ_ANNUALIZED']

            disp = style_df_for_display(df_us_gdp_recent,cols_gradient,rename_cols,cols_drop)
            tab1.write(disp)

            #TAB 2

            series = "QoQ"
            tab2.subheader("US GDP - QoQ")

            chart_settings = {
                "type": "bar",
                "title": "US GDP QoQ", 
                "xlabel": "Year", 
                "ylabel": "GDP QoQ", 
            }

            display_chart(chart_settings, df_us_gdp_all, series, tab2)

            chart_settings = {
                "type": "bar",
                "title": "US GDP QoQ - Last 10 Years", 
                "xlabel": "Year", 
                "ylabel": "GDP QoQ", 
            }

            display_chart(chart_settings, df_us_gdp_recent, series, tab2)

            cols_gradient = ['QoQ']
            rename_cols = {'DATE': 'Date'}
            cols_drop = ['gdpc1','YoY','QoQ_ANNUALIZED']

            disp = style_df_for_display(df_us_gdp_recent,cols_gradient,rename_cols,cols_drop)
            tab2.write(disp)

            #TAB 3
            series = "YoY"
            tab3.subheader("US GDP - YoY")

            chart_settings = {
                "type": "bar",
                "title": "US GDP YoY", 
                "xlabel": "Year", 
                "ylabel": "GDP YoY", 
            }

            display_chart(chart_settings, df_us_gdp_all, series, tab3)

            chart_settings = {
                "type": "bar",
                "title": "US GDP YoY - Last 10 Years", 
                "xlabel": "Year", 
                "ylabel": "GDP YoY", 
            }

            display_chart(chart_settings, df_us_gdp_recent, series, tab3)
            #tab3.write(df_us_gdp_recent)

            cols_gradient = ['YoY']
            rename_cols = {'DATE': 'Date'}
            cols_drop = ['gdpc1','QoQ','QoQ_ANNUALIZED']

            disp = style_df_for_display(df_us_gdp_recent,cols_gradient,rename_cols,cols_drop)
            tab3.write(disp)

            #TAB 4
            series = "QoQ_ANNUALIZED"
            tab4.subheader("US GDP - QoQ Annualized")

            chart_settings = {
                "type": "bar",
                "title": "US GDP QoQ Annualized", 
                "xlabel": "Year", 
                "ylabel": "GDP QoQ Annualized", 
            }

            display_chart(chart_settings, df_us_gdp_all, series, tab4)

            chart_settings = {
                "type": "bar",
                "title": "US GDP QoQ Annualized - Last 10 Years", 
                "xlabel": "Year", 
                "ylabel": "GDP QoQ Annualized", 
            }

            display_chart(chart_settings, df_us_gdp_recent, series, tab4)

            cols_gradient = ['QoQ_ANNUALIZED']
            rename_cols = {'DATE': 'Date'}
            cols_drop = ['gdpc1','QoQ','YoY']

            disp = style_df_for_display(df_us_gdp_recent,cols_gradient,rename_cols,cols_drop)
            tab4.write(disp)


        if option_lagging_indicator_charts == '005 - US Job Market':    
            pass
        if option_lagging_indicator_charts == '006 - PCE':
            pass
        if option_lagging_indicator_charts == '007 - US Inflation':
            pass
        if option_lagging_indicator_charts == '009 - US Industrial Production':
            pass
        if option_lagging_indicator_charts == '011 - US Durable Goods':
            pass
        if option_lagging_indicator_charts == '011 - US Retail Sales':
            pass

    if option_indicator_type == 'Interest Rates/FX':
        st.subheader(f'Interest Rates/FX')

        option_interest_rates_fx_charts = st.sidebar.selectbox("Charts", ('012 - Central Banks','014 - Money Supply'), 0)
        pass

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
            # gdpc1
            tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(tabs)

            df_sectors = get_data(table="macro_us_ism_manufacturing_sectors")           
            df_new_orders = get_data(table="macro_us_ism_manufacturing_new_orders")            
            df_production = get_data(table="macro_us_ism_manufacturing_production")            
            df_headline = get_data(table="macro_us_ism_manufacturing_headline")            

            df_sectors_last_6_months = df_sectors.tail(3).reset_index(drop=True)
            df_new_orders_last_6_months = df_new_orders.tail(3).reset_index(drop=True)
            df_production_last_6_months = df_production.tail(3).reset_index(drop=True)

            #TAB 1
            tab1.subheader(tabs[0])
            
            style_t1 = return_styled_ism_table1(df_sectors_last_6_months)
            tab1.write(style_t1)

            #Table containing sector values for last 6 months across all sectors
            #Chart showing sector values across all sectors for last 6 months
            series = "gdpc1"
            chart_settings = {
                "type": "line",
                "title": "Total US GDP", 
                "xlabel": "Year", 
                "ylabel": "GDP", 
            }

            #display_chart(chart_settings, df_us_gdp_all, series, tab1)

            #TAB 2
            tab2.subheader(tabs[1])

            style_t2 = return_styled_ism_table1(df_new_orders_last_6_months)
            tab2.write(style_t2)

            series = "gdpc1"
            chart_settings = {
                "type": "line",
                "title": "Total US GDP", 
                "xlabel": "Year", 
                "ylabel": "GDP", 
            }

            #display_chart(chart_settings, df_us_gdp_all, series, tab1)

            #TAB 3
            tab3.subheader(tabs[2])

            style_t3 = return_styled_ism_table1(df_production_last_6_months)
            tab3.write(style_t3)

            series = "gdpc1"
            chart_settings = {
                "type": "line",
                "title": "Total US GDP", 
                "xlabel": "Year", 
                "ylabel": "GDP", 
            }

            #display_chart(chart_settings, df_us_gdp_all, series, tab1)

            #TAB 4
            tab4.subheader(tabs[3])

            series = "gdpc1"
            chart_settings = {
                "type": "line",
                "title": "Total US GDP", 
                "xlabel": "Year", 
                "ylabel": "GDP", 
            }

            #display_chart(chart_settings, df_us_gdp_all, series, tab1)

            #TAB 5
            tab5.subheader(tabs[4])

            series = "gdpc1"
            chart_settings = {
                "type": "line",
                "title": "Total US GDP", 
                "xlabel": "Year", 
                "ylabel": "GDP", 
            }

            #display_chart(chart_settings, df_us_gdp_all, series, tab1)

            #TAB 6
            tab6.subheader(tabs[5])

            series = "gdpc1"
            chart_settings = {
                "type": "line",
                "title": "Total US GDP", 
                "xlabel": "Year", 
                "ylabel": "GDP", 
            }

            #display_chart(chart_settings, df_us_gdp_all, series, tab1)

            #TAB 7
            tab7.subheader(tabs[6])

            series = "gdpc1"
            chart_settings = {
                "type": "line",
                "title": "Total US GDP", 
                "xlabel": "Year", 
                "ylabel": "GDP", 
            }

            #display_chart(chart_settings, df_us_gdp_all, series, tab1)

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
                json_price_action = get_yf_price_action(symbol)

                dataSummaryDetail = json_price_action['quoteSummary']['result'][0]['summaryDetail']
                dataDefaultKeyStatistics = json_price_action['quoteSummary']['result'][0]['defaultKeyStatistics']
                dataSummaryProfile = json_price_action['quoteSummary']['result'][0]['summaryProfile']
                dataFinancialData = json_price_action['quoteSummary']['result'][0]['financialData']
                dataPrice = json_price_action['quoteSummary']['result'][0]['price']


                # Get High Level Company Details
                company_name = df_company_details['company_name'][0]
                sector = df_company_details['sector'][0]
                industry = df_company_details['industry'][0]
                exchange = df_company_details['exchange'][0]
                market_cap = dataPrice['marketCap']['fmt']
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
                #import pdb; pdb.set_trace()

                shares_outstanding_formatted = '{:,.2f}'.format(shares_outstanding).split('.00')[0]
                avg_vol_3m = df_yf_key_stats['avg_vol_3m'][0]
                avg_vol_10d = df_yf_key_stats['avg_vol_10d'][0]
                last = dataSummaryDetail['previousClose']['fmt']
                annual_high = dataSummaryDetail['fiftyTwoWeekHigh']['fmt']
                annual_low = dataSummaryDetail['fiftyTwoWeekLow']['fmt']
                percent_change_ytd = df_tickers_all.loc[df_tickers_all['Ticker']==symbol,'% Price Change (YTD)']
                percent_change_ytd =  percent_change_ytd.values[0]
                percent_change_ytd_formatted = '{:,.2f}%'.format(percent_change_ytd)
                moving_avg_50d = df_yf_key_stats['moving_avg_50d'][0]
                moving_avg_200d = df_yf_key_stats['moving_avg_200d'][0]
                #import pdb; pdb.set_trace()
                
                try:
                    div_yield = dataSummaryDetail['dividendYield']['fmt'] 
                except KeyError as e:
                    div_yield = None
                    #import pdb; pdb.set_trace()
                try:
                    beta = dataSummaryDetail['beta']['fmt']
                except KeyError as e:
                    beta = 0
                currency = dataSummaryDetail['currency']
                website = dataSummaryProfile['website']
                volume = dataSummaryDetail['volume']['longFmt'] 
                try:
                    target_price = dataFinancialData['targetHighPrice']['fmt']
                except KeyError as e:
                    target_price = None
                try:
                    next_fiscal_year_end = dataDefaultKeyStatistics['nextFiscalYearEnd']['fmt']
                except KeyError as e:
                    next_fiscal_year_end = None

                business_summary = dataSummaryProfile['longBusinessSummary']
                total_debt = dataFinancialData['totalDebt']['raw']
                ev = dataDefaultKeyStatistics['enterpriseValue']['fmt']
                #ev_formatted ='{:,.2f}'.format(ev)               
                try: 
                    days_to_cover_short_ratio = dataDefaultKeyStatistics['shortRatio']['raw']
                    days_to_cover_short_ratio_formatted ='{:,.2f}'.format(days_to_cover_short_ratio)

                except KeyError as e:
                    days_to_cover_short_ratio_formatted = None

                dividend_this_year = dataSummaryDetail['trailingAnnualDividendRate']['raw']
                dividend_this_year_formatted ='{:,.2f}'.format(dividend_this_year)

                column_names = ['Last','52 Week High','52 Week Low','YTD Change %','Market Cap', 'EV', 'Days to Cover', 'Target Price']
                column_data = [last, annual_high, annual_low, percent_change_ytd_formatted, market_cap, ev, days_to_cover_short_ratio_formatted, target_price]
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

                #col1,col2,col3 = st.columns(3)
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

                sort_cols = ['forecast_year']
                drop_rows = ['cid','id']
                rename_cols = {'sales': 'Sales','ebit': 'EBIT','net_income': 'Net Income','pe_ratio': 'PE Ratio','earnings_per_share': 'EPS','cash_flow_per_share': 'Cash Flow Per Share','book_value_per_share': 'Book Value Per Share','total_debt': 'Total Debt','ebitda': 'EBITDA', 'fcf': "FCF"}
                number_format_col = 'forecast_year'
                style_t4 = format_df_for_dashboard_flip(df_stockrow_stock_data, sort_cols, drop_rows, rename_cols, number_format_col)
                st.write(style_t4)

                st.markdown("""---""")

                st.markdown("Earnings Surprises")

                sort_cols = ['dt']
                drop_rows = ['cid','id', 'dt']
                rename_cols = {'reporting_priod': 'Reporting Period','eps_estimate': 'EPS Estimate','eps_reported': 'EPS Reported','sales_estimate': 'Sales Estimate','sales_reported': 'Sales Reported'}
                number_format_col = 'reporting_period'
                style_t5 = format_df_for_dashboard_flip(df_zacks_earnings_surprises, sort_cols, drop_rows, rename_cols, number_format_col)
                st.write(style_t5)

                st.markdown("""---""")

                col1,col2 = st.columns(2)

                col1.markdown("Geography") 
                sort_cols = ['revenue']
                drop_cols = ['cid','id']
                rename_cols = {'region': 'Region','revenue': 'Revenue'}
                format_cols = {'revenue': 'number'}

                if(len(df_zacks_product_line_geography) > 0):
                    style_t6 = format_df_for_dashboard(df_zacks_product_line_geography, sort_cols, drop_cols, rename_cols, format_cols=format_cols)
                    col1.write(style_t6)
                else:
                    col1.markdown("Geography data does not exist")

                col2.markdown("Peers")
                sort_cols = ['peer_ticker']
                drop_cols = ['cid','id' ]
                rename_cols = {'peer_company': 'Peer Company','peer_ticker': 'Peer Ticker'}
                format_cols = []

                style_t7 = format_df_for_dashboard(df_zacks_peer_comparison, sort_cols, drop_cols, rename_cols, format_cols=format_cols)
                col2.write(style_t7)

                st.markdown("""---""")

                st.markdown("Peer Comparison")

                #TODO: Get peer details and display it in a table
                df_peers = get_peer_details(df_zacks_peer_comparison)
                st.dataframe(df_peers)

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
        option_one_pager = st.sidebar.selectbox("Which Dashboard?", ('Volume','TA Patterns','Insider Trading', 'Country Exposure', 'Twitter'), 0)
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

            style_sectors = format_df_for_dashboard(df_vol_data_all_sectors, sort_cols, drop_cols, rename_cols, number_format_cols)

            col1.subheader(f'Volume by Sectors')
            col1.write(style_sectors, unsafe_allow_html=True)

            df_vol_data_all_industries = df_stock_volume.drop(['cid','id','sector','vs_avg_vol_10d','vs_avg_vol_3m', 'outlook', 'company_name', 'percentage_sold', 'last_close', 'symbol'], axis=1)
            df_vol_data_all_industries = df_vol_data_all_industries.groupby(['industry']).sum().sort_values(by=['last_volume'], ascending=False).reset_index()

            df_vol_data_all_industries = df_vol_data_all_industries.head(10)
            df_vol_data_all_industries = format_volume_df(df_vol_data_all_industries)

            sort_cols = []
            drop_cols = []
            rename_cols = {'industry': 'Industry','last_volume': 'Volume'}
            number_format_cols = []

            style_industries = format_df_for_dashboard(df_vol_data_all_industries, sort_cols, drop_cols, rename_cols, number_format_cols)
            col2.subheader(f'Volume by Industries')
            col2.write(style_industries)

            if(len(df_stock_volume) > 0):
                st.markdown("""---""")

                st.subheader(f'Volume by Individual Stocks')

                st.markdown(f'High Volume Vs Last 3 Months')
                df_stock_volume_3m = df_stock_volume.sort_values(by=['vs_avg_vol_3m'], ascending=False)        
                df_stock_volume_3m = df_stock_volume_3m[df_stock_volume['vs_avg_vol_3m'] > 1].reset_index()
                df_stock_volume_3m = format_volume_df(df_stock_volume_3m)    
                
                sort_cols = []
                order_cols = ['symbol','vs_avg_vol_10d','vs_avg_vol_3m', 'last_close', 'company_name', 'outlook']
                drop_cols = ['index','id', 'cid']
                rename_cols = {'vs_avg_vol_10d': '% Avg Vol 10d', 'vs_avg_vol_3m': '% Avg Vol 3m', 'outlook': 'Outlook', 'symbol': 'Symbol', 'last_close': 'Last', 'company_name': 'Company'}

                style_3m = format_df_for_dashboard(df_stock_volume_3m, sort_cols, drop_cols, rename_cols, order_cols=order_cols)                
                style_3m = style_3m.style.pipe(format_outlook)

                st.write(style_3m)
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
                    #TODO: Format each DF before printing
                    sort_cols = []
                    order_cols = []
                    drop_cols = []
                    rename_cols = {'symbol': 'Symbol', 'company_name': 'Company', 'sector': 'Sector', 'industry': 'Industry', 'percentage_sold': '% Traded Today', 'outlook': 'Outlook'}
                    number_format_cols = []

                    style_company_row = format_df_for_dashboard(df_company_row, sort_cols, drop_cols, rename_cols, number_format_cols, order_cols)                
                    if(row['outlook'] == 'bullish'):
                        style_company_row = style_company_row.style.pipe(format_outlook)
                    st.write(style_company_row)
                    st.image(f'https://finviz.com/chart.ashx?t={symbol}&ty=c&ta=1&p=d&s=l')
                    st.markdown("""---""")
            else:
                st.markdown("No Stock Volume Data Available")

            #st.markdown("Price Action Volume")
            #st.dataframe(df)

        if option_one_pager == 'TA Patterns':        
            st.subheader(f'TA Patterns')
            df = get_data(table="ta_patterns")            
            df_consolidating = df.loc[df['pattern'] == 'consolidating']
            df_breakout = df.loc[df['pattern'] == 'breakout']

            st.markdown("Consolidating")
            data = {'ticker':[],'pattern':[]}
            for index, row in df_consolidating.iterrows():
                symbol = row['ticker']
                df_temp = pd.DataFrame(data)
                temp_row = [row['ticker'],row['pattern']]
                df_temp.loc[len(df.index)] = temp_row

                sort_cols = []
                order_cols = []
                drop_cols = []
                rename_cols = {'ticker': 'Ticker', 'pattern': 'Pattern'}
                number_format_cols = []

                style_company_row = format_df_for_dashboard(df_temp, sort_cols, drop_cols, rename_cols, number_format_cols, order_cols)                
                st.write(style_company_row)
                st.image(f'https://finviz.com/chart.ashx?t={symbol}&ty=c&ta=1&p=d&s=l')

            st.markdown("Breakout")
            data = {'ticker':[],'pattern':[]}
            for index, row in df_breakout.iterrows():
                symbol = row['ticker']
                df_temp = pd.DataFrame(data)
                temp_row = [row['ticker'],row['pattern']]
                df_temp.loc[len(df.index)] = temp_row

                sort_cols = []
                order_cols = []
                drop_cols = []
                rename_cols = {'ticker': 'Ticker', 'pattern': 'Pattern'}
                number_format_cols = []

                style_company_row = format_df_for_dashboard(df_temp, sort_cols, drop_cols, rename_cols, number_format_cols, order_cols)                
                st.write(style_company_row)
                st.image(f'https://finviz.com/chart.ashx?t={symbol}&ty=c&ta=1&p=d&s=l')

        if option_one_pager == 'Insider Trading':        
            st.subheader(f'Insider Trading')
            df = get_data(table="macro_insidertrading")

            sort_cols = []
            order_cols = ['filing_date','company_ticker','company_name', 'insider_name', 'insider_title', 'trade_type', 'trade_price', 'percentage_owned']
            drop_cols = ['id']
            rename_cols = {'filing_date': 'Filing Date', 'company_ticker': 'Ticker', 'company_name': 'Company', 'insider_name': 'Insider', 'insider_title': 'Title', 'trade_type': 'Trade', 'trade_price': 'Price', 'percentage_owned': '% Owned'}

            style_insider_trading = format_df_for_dashboard(df, sort_cols, drop_cols, rename_cols, order_cols=order_cols)                

            st.write(style_insider_trading)

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