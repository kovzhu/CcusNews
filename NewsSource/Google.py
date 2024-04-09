import requests
import pandas as pd 
from bs4 import BeautifulSoup as bs 


from CcusNews.const import HEADERS 


class GoogleNews:
    
    def __init__(self, days_ahead) -> None:
        # self.base_url = r'https://news.google.com/search?q=CCUS%20when%3A7d&hl=en-US&gl=US&ceid=US%3Aen'
        self.base_url = r'https://news.google.com/search?q='
        self.days_ahead = days_ahead
        self.keywords = ['CCUS', 'Carbon storage', 'Carbon capture', 'CO2 capture']


    def get_news1(self, url):
        r = requests.get(url, headers=HEADERS, verify=False)
        soup = bs(r.text.encode('utf-8'), 'html.parser')
        news = soup.find_all(class_ = 'JtKRv')
        
        df = pd.DataFrame()
        for item in news:
            s =  pd.Series({'title': item.text, 
                'url': 'https://news.google.com' + item.get('href')[1:]})
            df = pd.concat([df,s], axis=1)
        df = df.T
        
        return df
    
    def get_news(self, url):
        r = requests.get(url, headers=HEADERS, verify=False)
        soup = bs(r.text.encode('utf-8'), 'html.parser')
        news_blocks = soup.find_all(class_ = 'PO9Zff Ccj79 kUVvS')
        
        df = pd.DataFrame()
        for block in news_blocks:
            news = block.find_all(class_ = 'JtKRv')[0]
            time = block.find_all(class_ = 'hvbAAd')[0]['datetime']
            # time = pd.to_datetime(time)
            s = pd.Series({'title': news.text, 
                'url': 'https://news.google.com' + news.get('href')[1:],
                'date_published':time})
            df = pd.concat([df,s], axis=1)
        df = df.T
        print(f"{len(df)} news retreived from the search {url}")
        return df
    
    def url_generator(self, keyword, days_ahead):
        url = self.base_url + str(keyword) + '%20when%3A' + str(days_ahead) + 'd&&hl=en-US&gl=US&ceid=US%3Aen'
        
        return url
    
    def get_all_news(self):
        df = pd.DataFrame()
        for keyword in self.keywords:
            url = self.url_generator(keyword, self.days_ahead)
            news = self.get_news(url).reindex()
            df = pd.concat([df, news])
        df = df.drop_duplicates(subset=['title'])
        
        return df