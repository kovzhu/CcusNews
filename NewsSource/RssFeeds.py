import feedparser
import pandas as pd

import datetime

class FeedData:
    '''
    Get rss feed news for a single rss feed
    '''
    def __init__(self,rss_feed) -> None:
        self._rss_feed = rss_feed
        
        
    def _get_rss_news(self) -> pd.DataFrame:
        '''
        Get the updated RSS feed data
        '''
        # self.feeds = self.feeds.head(3)
        news = pd.DataFrame()
        try:
            news = feedparser.parse(self._rss_feed)
            feed_data = pd.DataFrame()
            for entry in news.entries:
                s = pd.Series({'title':entry.title, 'link':entry.link, 'description':entry.description, 'published':entry.published})
                feed_data = pd.concat([feed_data, s], axis=1)
            feed_data=feed_data.T
        except Exception as e:
            print(f"Feed parsing failed: {self._rss_feed}, {e}")
            feed_data = pd.DataFrame()
        
        # convert the time format
        if 'published' in feed_data.columns:
            # feed_data.published = feed_data.published.apply(lambda x: datetime.datetime.fromtimestamp(x))
            feed_data.published = feed_data.published.apply(lambda x: pd.to_datetime(x, errors='coerce'))
            feed_data.published = feed_data.published.apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
        
        return feed_data
    
    @property
    def feed_data(self):
        return self._get_rss_news()
    

class BatchFeedData:
    
    def __init__(self, df:pd.DataFrame) -> None:
        self._df = df
        self._FeedData = FeedData
        
    def _get_batch_rss_news(self):
        '''
        Get news from rss feeds in batch
        '''
        
        # get the list of the feeds from the input
        if 'Feeds' in self._df.columns:
            feeds_list = self._df.Feeds
        else:
            print('Inputed feeds list are not accurate')
            feeds_list = pd.DataFrame()
        
        # loop through the feeds list and combine the data
        df = pd.DataFrame()
        for feed in feeds_list:
            feed_news = self._FeedData(feed).feed_data
            df = pd.concat([df, feed_news])
        
        return df 
    
    @property
    def FeedsData(self):
        news = self._get_batch_rss_news()
        if len(news):
            print(f"{len(news)} news were collected from {len(self._df)} Rss feeds")
            return news
        else:
            print(f'No news collected for rss feeds')
            return news