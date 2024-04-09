import requests
import pandas as pd 
import datetime
import json

from requests.packages.urllib3.exceptions import InsecureRequestWarning

# Suppress only the single warning we're interested in
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

class YahooNews():
    '''
    get the news of a company by ticker
    '''
    def __init__(self, ticker:str) -> None:
        self._news = None
        self._ticker = ticker
        self._base_url = r'https://query2.finance.yahoo.com/v1/finance/search?q='
    
    @property
    def news(self):
        if self._news:
            return self._news
        else:
            self._news = self._get_news()
            return self._news
        
    def _get_news(self):
        data = requests.get(
            url=self._base_url+str(self._ticker),
            headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'},
            verify=False,
            timeout=120
        )
        
        # dic = data.json()        
        dic = json.loads(data.text)        
        news = dic.get('news',[''])
        
        # convert the list of dictionaries into a dataframe
        df = pd.DataFrame()
        for one_news in news:
            s = pd.Series(one_news)
            df = pd.concat([df, s], axis='columns')
        df = df.T
        df['searched_ticker'] = self._ticker
        
        try:
            df['providerPublishTime'] = df.providerPublishTime.apply(lambda x: datetime.datetime.fromtimestamp(x))
            df.providerPublishTime = df.providerPublishTime.apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') ) # convert into a string so can be used in sql
        except Exception as e:
            print(f'The time formatting of {self._ticker} failed') 
        
        # remove the thumbnail columns
        if 'thumbnail' in df.columns:
            df = df.drop('thumbnail', axis=1) 
        if 'relatedTickers' in df.columns:
            df['relatedTickers'] = df.relatedTickers.apply(lambda x: str(x)[1:-1]) # covert the list style string into normal ones, so SQL can inject the data, or else there will be an error

        
        print(f"{len(df)} news for {self._ticker} from Yahoo finance")
        
        return df
    

class BatchYahooNews:
    '''
    Get the news by tickers in batches, with the imported dataframe
    
    '''
    def __init__(self, df: pd.DataFrame) -> None:
        self._Yahoo_news = YahooNews
        self._df = df
        
    
    def _get_batch_news(self) -> pd.DataFrame:
        if ('Ticker codes' in self._df.columns) and ('Exchanges' in self._df.columns):
            df = pd.DataFrame()
            for _,row in self._df.iterrows():
                news = self._Yahoo_news(row['Ticker codes']).news # get the news for a single company
                news['exchange'] = row['Exchanges'] # add the exchange data if there is one
                # news['searched_ticker'] = row['Ticker codes']
                # news['company'] = row['Company name']
                df = pd.concat([df,news])
            return df 
        else:
            print('The company tickers are not properly imported')
    
    @property
    def BatchNews(self):
        news = self._get_batch_news()
        print(f"Total {len(news)} of news were collected from Yahoo Finance")
        return news
                

