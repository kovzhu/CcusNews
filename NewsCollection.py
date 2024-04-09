import pandas as pd 
# from functools import lru_cache

import os
import datetime

from .NewsSource.YahooNews import BatchYahooNews
from .NewsSource.RssFeeds import BatchFeedData
from .NewsSource.GepsNews import GepsNews
from .NewsSource.Google import GoogleNews
from .database import Database
from .utils import keywords_check, unpivot_keywords

class NewsCollect():
    
    def __init__(self) -> None:
        
        '''
        Get the lists of sources and keywords
        '''
        self._current_path = os.path.dirname(__file__)
        self._timestamp = datetime.datetime.strftime(datetime.datetime.now(),'%d-%b-%Y') # can be used in file name
        self._timestamp2 = datetime.datetime.strftime(datetime.datetime.now(),"%Y-%m-%d %H:%M:%S")
        # self._file_path = os.path.join(self._current_path,'CCUS news folder')

        self._tickers = pd.read_excel(os.path.join(self._current_path,'data','CCUS sources and keywords.xlsx'), sheet_name='Ticker Codes') 
        self._feeds = pd.read_excel(os.path.join(self._current_path,'data','CCUS sources and keywords.xlsx'), sheet_name='RSS feeds') 
        # keywords columns: ['Keywords', 'Keyword_type', 'ranking']
        self._keywords = pd.read_excel(os.path.join(self._current_path,'data','CCUS sources and keywords.xlsx'), sheet_name='Keywords')
        
        # for testing purpose
        # self._tickers = self._tickers.iloc[:3,:]
        # self._feeds = self._feeds.iloc[:3,:]
        
        self._days_ahead = 8
        
        
        self._YahooNews = BatchYahooNews(self._tickers) # instantiate the BatchYahooNews with the ticker
        self._FeedsData = BatchFeedData(self._feeds) # instantiate the BatchFeedData from the imported list of feeds
        self._GepsNews = GepsNews(days_ahead= self._days_ahead)
        self._GoogleNews = GoogleNews(days_ahead= self._days_ahead)
        
        self._database = Database()
        
        # store the data to avoid repeated data fetch
        self._yahoo_news = None
        self._rss_feeds_news = None 
        self._geps_news = None
        self._google_news = None
        

    @property
    def yahoo_news(self):
        '''
        data columns:
        ['uuid', 'title', 'publisher', 'link', 'providerPublishTime', 'type',
        'thumbnail', 'relatedTickers', 'searched_ticker', 'exchange',
        'keywords']
        '''
        if self._yahoo_news is None:
            yh_news =  self._YahooNews.BatchNews 
            # add the keywords column
            yh_news['keywords'] = yh_news.apply(lambda x: keywords_check(self._keywords.Keywords.to_list(),x['title']), axis=1)
            yh_news = unpivot_keywords(yh_news) # unpivot the keywords
            self._yahoo_news = yh_news
        else:
            yh_news = self._yahoo_news
            
        yh_news = yh_news.rename(columns={'providerPublishTime':'date_published'})
        
        self._database.database_write(yh_news,'yahoo_news_data')
        
        return yh_news
    
    @property
    def rss_feeds_news(self):
        '''
        data columns:
        ['title', 'link', 'description', 'published']
        '''
        if self._rss_feeds_news is None:
            feeds_data = self._FeedsData.FeedsData
            # add the keywords columns
            feeds_data['keywords'] = feeds_data.apply(lambda x: keywords_check(self._keywords.Keywords.to_list(),x['title']+x['description']), axis=1)
            feeds_data = unpivot_keywords(feeds_data) # unpivot the keywords
            self._rss_feeds_news = feeds_data
        else:
            feeds_data = self._rss_feeds_news
        
        # change the column name to a consistent name across tables
        feeds_data = feeds_data.rename(columns={'published':'date_published'})
        
        # archive in the database
        self._database.database_write(feeds_data, 'rss_data')
        
        return feeds_data
    
    @property
    def geps_news(self):
        '''
        data columns:
        ['upstream_intelligence_id', 'title', 'document_url', 'published_date',
        'original_published_date', 'snipped_text']
        '''
        if self._geps_news is None:
            geps_news = self._GepsNews.GepsNews
            self._geps_news = geps_news
        else:
            geps_news = self._geps_news
        
        # define the unified date_published colume
        geps_news = geps_news.rename(columns={'published_date':'date_published'})
        geps_news['keywords'] = geps_news.apply(lambda x: keywords_check(self._keywords.Keywords.to_list(),x['title']), axis=1)
            
        self._database.database_write(geps_news,'geps_news')
        
        return geps_news

    @property
    def google_news(self):
        if self._google_news is None:
            google_news = self._GoogleNews.get_all_news()
            self._google_news = google_news
        else:
            google_news = self._google_news
        
        google_news['keywords'] = google_news.apply(lambda x: keywords_check(self._keywords.Keywords.to_list(),x['title']), axis=1)
        google_news = unpivot_keywords(google_news)
        
        self._database.database_write(google_news,'google_news')
        return google_news

    @property
    def geps_pdf_reports(self):
        
        return self._GepsNews.GepsPdfReports



