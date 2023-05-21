CREATE TABLE IF NOT EXISTS Company (
    cid SERIAL,
    symbol TEXT NOT NULL,
    company_name TEXT NULL,
    sector TEXT NULL,
    industry TEXT NULL,
    exchange TEXT NOT NULL,
    market_cap NUMERIC,
    shares_outstanding NUMERIC,
    PRIMARY KEY (symbol),
    UNIQUE(cid)
);

CREATE TABLE IF NOT EXISTS BalanceSheet (
    id SERIAL,
    cid INTEGER NOT NULL,
    dt TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    preferred_stock NUMERIC,
    common_stock_par NUMERIC NOT NULL,
    capital_surplus NUMERIC NOT NULL,
    retained_earnings NUMERIC NOT NULL,
    other_equity NUMERIC NOT NULL,
    treasury_stock NUMERIC NOT NULL,
    total_shareholders_equity NUMERIC NOT NULL,
    total_liabilities_shareholders_equity NUMERIC NOT NULL,
    total_common_equity NUMERIC NOT NULL,
    shares_outstanding NUMERIC NOT NULL,
    book_value_per_share NUMERIC NOT NULL,
    reporting_period TEXT NOT NULL,
    PRIMARY KEY (cid, dt, reporting_period),
    CONSTRAINT fk_balance_sheet FOREIGN KEY (cid) REFERENCES Company (cid)
);

CREATE TABLE IF NOT EXISTS EarningsSurprise (
    id SERIAL,
    cid INTEGER NOT NULL,
    dt TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    reporting_period TEXT NOT NULL,
    eps_estimate NUMERIC NOT NULL,
    eps_reported NUMERIC NOT NULL,
    sales_estimate NUMERIC NOT NULL,
    sales_reported NUMERIC NOT NULL,
    PRIMARY KEY (cid, dt, reporting_period),
    CONSTRAINT fk_earnings_surprise FOREIGN KEY (cid) REFERENCES Company (cid)
);

CREATE TABLE IF NOT EXISTS CompanyGeography (
    id SERIAL,
    cid INTEGER NOT NULL,
    region TEXT NOT NULL,
    revenue TEXT NOT NULL,
    PRIMARY KEY (cid, region),
    CONSTRAINT fk_geography FOREIGN KEY (cid) REFERENCES Company (cid)
);

CREATE TABLE IF NOT EXISTS CompanyRatio (
    id SERIAL,
    cid INTEGER NOT NULL,
    pe TEXT NULL,
    eps_ttm TEXT NULL,
    pe_forward TEXT NULL,
    eps_y1 TEXT NULL,
    peg TEXT NULL,
    eps_y0 TEXT NULL,
    price_book TEXT NULL,
    price_sales TEXT NULL,
    target_price TEXT NULL,
    roe TEXT NULL,
    range_52w TEXT NULL,
    quick_ratio TEXT NULL,
    gross_margin TEXT NULL,
    current_ratio TEXT NULL,
    PRIMARY KEY (cid),
    CONSTRAINT fk_companyratio FOREIGN KEY (cid) REFERENCES Company (cid)
);

CREATE TABLE IF NOT EXISTS CompanyForecast (
    id SERIAL,
    cid INTEGER NOT NULL,
    forecast_year TEXT NOT NULL,
    sales NUMERIC,
    ebit NUMERIC,
    net_income NUMERIC,
    pe_ratio NUMERIC,
    earnings_per_share NUMERIC,
    cash_flow_per_share NUMERIC,
    book_value_per_share NUMERIC,
    total_debt NUMERIC,
    ebitda NUMERIC,
    fcf NUMERIC,
    PRIMARY KEY (cid, forecast_year),
    CONSTRAINT fk_companyforecast FOREIGN KEY (cid) REFERENCES Company (cid)
);

CREATE TABLE IF NOT EXISTS CompanyMovingAverage (
    id SERIAL,
    cid INTEGER NOT NULL,
    market_cap TEXT NOT NULL,
    ev TEXT NULL,
    avg_vol_3m TEXT NULL,
    avg_vol_10d TEXT NULL,
    moving_avg_50d NUMERIC,
    moving_avg_200d NUMERIC,
    ev_revenue TEXT NULL,
    ev_ebitda TEXT NULL,
    price_book TEXT NULL,
    PRIMARY KEY (cid),
    CONSTRAINT fk_companyforecast FOREIGN KEY (cid) REFERENCES Company (cid)
);

CREATE TABLE IF NOT EXISTS CompanyPeerComparison (
    id SERIAL,
    cid INTEGER NOT NULL,
    peer_company TEXT NULL,
    peer_ticker TEXT NOT NULL,
    PRIMARY KEY (cid, peer_ticker),
    CONSTRAINT fk_companyforecast FOREIGN KEY (cid) REFERENCES Company (cid)
);

CREATE TABLE IF NOT EXISTS Macro_EarningsCalendar (
    id SERIAL,
    dt TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    dt_time TEXT NOT NULL,
    ticker TEXT NOT NULL,
    company_name TEXT NOT NULL,
    market_cap_mil NUMERIC NOT NULL
);

CREATE TABLE IF NOT EXISTS CompanyPriceAction (
    id SERIAL,
    cid INTEGER NOT NULL,
    last_volume NUMERIC,
    vs_avg_vol_10d NUMERIC,
    vs_avg_vol_3m NUMERIC,
    outlook TEXT NULL,
    percentage_sold NUMERIC,
    last_close NUMERIC,
    PRIMARY KEY (cid),
    CONSTRAINT fk_companypriceaction FOREIGN KEY (cid) REFERENCES Company (cid)
);


CREATE TABLE IF NOT EXISTS Macro_EconomicCalendar (
    id SERIAL,
    dt TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    dt_time TEXT NOT NULL,
    country TEXT NOT NULL,
    economic_event TEXT NOT NULL,
    previous TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS Macro_WhitehouseAnnouncement (
    id SERIAL,
    dt TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    post_title TEXT NOT NULL,
    post_url TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS Macro_GeopoliticalCalendar (
    id SERIAL,
    event_date TEXT NOT NULL,
    event_name TEXT NOT NULL,
    event_location TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS TA_Patterns (
    id SERIAL,
    ticker TEXT NOT NULL,
    pattern TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS Macro_InsiderTrading (
    id SERIAL,
    filing_date TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    company_ticker TEXT NOT NULL,
    company_name TEXT NOT NULL,
    insider_name TEXT NOT NULL,
    insider_title TEXT NOT NULL,
    trade_type TEXT NOT NULL,
    trade_price TEXT NOT NULL,
    percentage_owned NUMERIC    
);