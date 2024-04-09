import requests
import pandas as pd
import json

import datetime
from CcusNews.utils import getResponse


class GepsNews():
    
    def __init__(self, days_ahead = 8) -> None:
        self._days_ahead = days_ahead
        self._selected_columns = ['upstream_intelligence_id',
                                    'title', 
                                    'document_url',  
                                    'published_date',
                                    'original_published_date', 
                                    'snipped_text']
        
        self._geps_news = None
        self._GepsNews = None
        self._GepsPdfReports = None
        
    
    def _generate_query_url(self):
        special_interest = 'CO2 and CCS'
        start_datetime = datetime.datetime.now() - datetime.timedelta(days=self._days_ahead) # get the updates within 8 days; can be changed as needed
        start_date_string = start_datetime.strftime('%Y-%m-%d')
        # url = 'https://energydataservices.ihsenergy.com/rest/data/v1/upstreamintelligence/documents?$filter=product_type:GEPS AND special_interests: "CO2 and CCS"&$orderby=updated_date desc'
        url = ('https://energydataservices.ihsenergy.com/rest/data/v1/upstreamintelligence/documents?$'
                'filter=product_type: "GEPS" '
                'AND published_date GT '
                f'"{start_date_string}T00:00:00" '
                f'AND special_interests: "{special_interest}"'
                '&$orderby=updated_date desc')   
        
        return url
    
    def _get_GEPS_news(self):
        
        url = self._generate_query_url()
        # print(url)
        response = getResponse(url)
        
        
        # df = pd.DataFrame(response.json()).get('elements','No data') # this doesn't work - don't know why
        df = pd.DataFrame(json.loads(response.text.encode('utf-8')).get('elements','No data'))
        try:
            df =df[self._selected_columns]
        except KeyError:
            df = pd.DataFrame()
            print(f'No GEPS report available, {KeyError}')
        
        return df 
    

    def _get_GEPS_report(self):
        '''
        Get the pdf reports in the Geps news
        '''
        geps_pdfs = []
        
        if self._geps_news is None:
            self._geps_news = self._get_GEPS_news()

        try:
            document_urls = list(self._geps_news.document_url)
        except KeyError as e:
            document_urls = [] 
            print(f"Error in getting the report urls; {e}")
        
        if len(document_urls)!=0: 
            for url in document_urls:
                try:
                    geps_pdfs.append(getResponse(url))
                except:
                    pass
            
            print(f'{len(geps_pdfs)} GEPS pdf reports were collected')
        
        return geps_pdfs

    @property
    def GepsNews(self):
        if self._GepsNews is None:
            self._GepsNews = self._get_GEPS_news()
        return self._GepsNews

    @property
    def GepsPdfReports(self) -> list:
        if self._GepsPdfReports is None:
            self._GepsPdfReports = self._get_GEPS_report()
        
        return self._GepsPdfReports

