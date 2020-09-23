#################################################
# CorpDataPuller.py
#################################################
# Description:
# * Pull historical returns for given ticker and
# time period.

import dateutil.parser as dtparser
from datetime import datetime, date, timedelta
import lxml
from lxml import html
from math import log
import requests
import numpy as np
from pandas import DataFrame
import yfinance as yf
import requests
import os

class CorpDataPuller(object):
    """
    * Pull historical returns, company data for ticker.
    """
    __haveAPIKeys = False
    #__session = requests_cache.CachedSession(cache_name = 'cache', backend = 'sqlite', expire_after = timedelta(days=3))
    __validPriceTypes = set(['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume'])
    __validTradeAttributes = set(['language', 'region', 'quoteType', 'triggerable', 'quoteSourceName', 'currency', 'tradeable', 'exchange', 'shortName', 'longName', 'messageBoardId', 
                         'exchangeTimezoneName', 'exchangeTimezoneShortName', 'gmtOffSetMilliseconds', 'market', 
                         'esgPopulated', 'firstTradeDateMilliseconds', 'priceHint', 'postMarketChangePercent', 'postMarketTime', 'postMarketPrice', 'postMarketChange', 
                         'regularMarketChange', 'regularMarketChangePercent', 'regularMarketTime', 'regularMarketPrice', 'regularMarketDayHigh', 'regularMarketDayRange', 
                         'regularMarketDayLow', 'regularMarketVolume', 'regularMarketPreviousClose', 'bid', 'ask', 'bidSize', 'askSize', 'fullExchangeName', 
                         'financialCurrency', 'regularMarketOpen', 'averageDailyVolume3Month', 'averageDailyVolume10Day', 'fiftyTwoWeekLowChange', 
                         'fiftyTwoWeekLowChangePercent', 'fiftyTwoWeekRange', 'fiftyTwoWeekHighChange', 'fiftyTwoWeekHighChangePercent', 'fiftyTwoWeekLow', 
                         'fiftyTwoWeekHigh', 'dividendDate', 'earningsTimestamp', 'earningsTimestampStart', 'earningsTimestampEnd', 'trailingAnnualDividendRate', 
                         'trailingPE', 'trailingAnnualDividendYield', 'marketState', 'epsTrailingTwelveMonths', 'epsForward', 'sharesOutstanding', 
                         'bookValue', 'fiftyDayAverage', 'fiftyDayAverageChange', 'fiftyDayAverageChangePercent', 'twoHundredDayAverage', 'twoHundredDayAverageChange', 
                         'twoHundredDayAverageChangePercent', 'marketCap', 'forwardPE', 'priceToBook', 'sourceInterval', 'exchangeDataDelayedBy', 'symbol'])
    __validCompanyAttributes = { 'sector' , 'industry', 'full time employees' }
    __validYFinanceAttributes = { key.lower() : key for key in __validTradeAttributes }
    def __init__(self, priceTypes = None, tradeAttributes = None, companyAttributes = None):
        """
        * Instantiate new object.
        """
        CorpDataPuller.__Validate(priceTypes, tradeAttributes, companyAttributes)
        self.__SetProperties(priceTypes, tradeAttributes, companyAttributes)
    
    #################
    # Properties:
    #################
    @property
    def TradeAttributes(self):
        return self.__tradeAttributes.copy()
    @property
    def PriceTypes(self):
        return self.__priceTypes.copy()
    @property
    def CompanyAttributes(self):
        return self.__allAttributes.copy()

    #################
    # Interface Methods:
    #################
    def GetAttributes(self, ticker):
        """
        * Get attributes of company with ticker.
        Inputs:
        * ticker: String with company ticker.
        Optional Inputs:
        * attributes: Put 'all' if want all possible attributes. Otherwise must be string in ValidAttributes().
        Outputs:
        * Returns map containing { Attr -> Value }.
        """
        errs = []
        if not isinstance(ticker, str):
            errs.append('ticker must be string.')
        if attributes:
            result = self.__CheckAttrs(attributes)
            if result:
                errs.extend(result)
        if errs:
            raise Exception('\n'.join(errs))
        # Get requested attributes for company:
        output = {}
        ticker = ticker.upper()
        if self.__YFinAttrs:
            data = yf.Ticker(ticker)
            for attr in self.__YFinAttrs:
                if attr not in data.info:
                    val = None
                else:
                    val = data.info[attr]
                val = val if not isinstance(val, str) else val.strip()
                output[attr] = val
        
        if self.__RequestAttrs:
            url = 'https://finance.yahoo.com/quote/%s/profile?p=%s' % (ticker, ticker)
            result = requests.get(url)
            tree = html.fromstring(result.content)
            for label in self.__RequestAttrs:
                cap = label.capitalize()
                xp = f"//span[text()='{cap}']/following-sibling::span[1]"
                s = None
                try:
                    s = tree.xpath(xp)[0]
                    val = s.text_content()
                except:
                    val = None
                output[label] = val

        output = { key.lower() : output[key] for key in output }
        
        return output

    def GetAssetPrices(self, ticker, startDate, endDate):
        """
        * Get prices of security with ticker between dates.
        Inputs:
        * startDate: Expecting datetime/string with format YYYY-MM-DD.
        * endDate: Expecting datetime/string with format YYYY-MM-DD. 
        * ticker: string security ticker or list.
        Output:
        * Returns dataframe filled with asset prices with Date and PriceTypes as columns for ticker.
        """
        errs = []
        if not isinstance(ticker, (str, list)):
            errs.append('ticker must be a string or list of strings.')
        elif isinstance(ticker, str):
            ticker = [ticker]
        startDate, endDate, dateErrs = CorpDataPuller.__ConvertDates(startDate, endDate)
        errs.extend(dateErrs)
        if errs:
            raise BaseException('\n'.join(errs))
        # Swap date order if necessary:
        if startDate > endDate:
            copy = endDate
            endDate = startDate
            startDate = copy
        
        coldiffs = set(CorpDataPuller.__validPriceTypes) - set(self.__priceTypes) 
        prices = yf.download(tickers = ticker, start = startDate.strftime('%Y-%m-%d'), end = endDate.strftime('%Y-%m-%d'))
        # Return prices with only requested columns:
        return prices.drop([col for col in prices.columns if col[0] in coldiffs], axis = 1)

    ###################
    # Private Helpers:
    ###################
    @staticmethod
    def __Validate(priceTypes, tradeAttributes, companyAttributes):
        """
        * Validate constructor parameters.
        """
        errs = []
        vals = {'priceTypes' : (priceTypes, CorpDataPuller.__validPriceTypes), 
                'tradeAttributes' : (tradeAttributes, CorpDataPuller.__validTradeAttributes), 
                'companyAttributes' : (companyAttributes, CorpDataPuller.__validCompanyAttributes)}
        for val in vals:
            if vals[val][0] is None:
                continue
            if not isinstance(vals[val][0], str) and not hasattr(vals[val][0], '__iter__'):
                errs.append('%s must be a string or an iterable of strings.' % val)
            elif hasattr(vals[val][0], '__iter__') and any([not isinstance(v, str) for v in vals[val][0]]):
                errs.append('%s must only contain strings.' % val)
            else:
                valid = { v.capitalize() for v in vals[val][1] }
                invalid = { v.capitalize() for v in vals[val][0] } - valid
                if invalid:
                    errs.append('The following are invalid for %s: %s' % (val, ','.join(invalid)))
        if errs:
            raise ValueError('\n'.join(errs))

    def __SetProperties(self, priceTypes, tradeAttributes, companyAttributes):
        """
        * Set properties from constructor parameters.
        """
        self.__priceTypes = ['Adj Close'] if priceTypes is None else list(priceTypes)
        self.__companyAttributes = [] if companyAttributes is None else list(companyAttributes)
        self.__tradeAttributes = [] if tradeAttributes is None else list(tradeAttributes)

    ###################
    # Static Methods:
    ###################
    @staticmethod
    def ValidAttributes():
        return list(CorpDataPuller.__validAttributes.keys())
    @staticmethod
    def ValidPriceTypes():
        return list(__validPriceTypes.keys())
    @staticmethod
    def __ConvertDates(startDate, endDate):
        errs = []
        if not isinstance(startDate, (date, datetime, str)):
            errs.append('startDate must be date/datetime/"YYYY-MM-DD" string.')
        elif isinstance(startDate, str):
            try:
                startDate = dtparser.parse(startDate)
            except:
                errs.append('startDate must be "YYYY-MM-DD".')
        if not isinstance(endDate, (date, datetime, str)):
            errs.append('endDate must be date/datetime/"YYYY-MM-DD" string.')
        elif isinstance(endDate, str):
            try:
                endDate = dtparser.parse(endDate)
            except:
                errs.append('endDate must be "YYYY-MM-DD".')
            
        return (startDate, endDate, errs)
    @staticmethod
    def __CalcReturns(prices, method):
        earliestDate = min(prices.index)
        returns = method(prices, prices.shift(1))
        # Drop first row:
        return returns.drop(index = min(returns.index))

if __name__ == '__main__':
    import dateutil.parser as dtparse
    from pandas import read_csv
    import pickle

    if not os.path.exists(r'Data\AllData.csv'):
        prices = ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
        puller = CorpDataPuller(priceTypes = prices)
        tickers = list(read_csv('Data\GetTickersSP500.csv')['ticker'])
        tickers_fixed = [tk.strip('.US Equity') for tk in tickers]
        colLabels = []
        for price in prices:
            for ticker in tickers:
                colLabels.append((price, ticker))
        prices = puller.GetAssetPrices(tickers_fixed,'2006-01-01','2017-12-31')
        #prices = prices.columns.rename({tickers_fixed[i] : tickers[i] for i in range(0, len(tickers))}, level = 1)
        prices.to_csv('Data\AllData.csv')
        pickled = { colLabels[i] : list(prices[col]) for i, col in enumerate(prices.columns)}
        pickle.dumps(pickled, open('AllData.pkl', 'r'))

