
import requests
import pandas as pd 

# import nltk
# from nltk.tokenize import word_tokenize
# from nltk.corpus import stopwords
# from nltk.probability import FreqDist
# import string

import sys
from requests.packages.urllib3.exceptions import InsecureRequestWarning

# Suppress only the single warning we're interested in
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

from .const import USER_NAME, PASS_WORD

def getResponse (url,un = USER_NAME, pw = PASS_WORD, verify = False):
    '''
    Do the requests.get() and return the response
    '''
    auth = requests.auth.HTTPBasicAuth(un,pw)

    headers = {
    'Accept': 'application/json, application/pdf',
    'User-Agent': 'PostmanRuntime/7.26.8'}
    # params= {}
    
    try:
        # response = requests.request("GET", url, headers=headers, params=params)
        response = requests.get(url=url, headers=headers, auth=auth, stream=True, verify= verify, timeout=30)
        # return response
        reqIsJson = False
        reqIsPdf = False

        if "application/json" in response.headers.get('content-type'):
            reqIsJson = True

        if "application/pdf" in response.headers.get('content-type'):
            reqIsPdf = True

        # if response.status_code == 200 and (reqIsJson or reqIsPdf):
        if response.status_code == 200:
            return response

        if response.status_code == 200 and (reqIsJson and reqIsPdf) == False:
            print("Unsupported content type received : ", response.headers.get('content-type'))
            # sys.exit()

        print('Status Code: ' + str(response.status_code))

        if response.status_code == 400:
            print("The server could not understand your request, check the syntax for your query.")
            print('Error Message: ' + str(response.json()))
        elif response.status_code == 401:
            print("Login failed, please check your user name and password.")
        elif response.status_code == 403:
            print("You are not entitled to this data.")
        elif response.status_code == 404:
            print("The URL you requested could not be found or you have an invalid view name.")
        elif response.status_code == 500:
            print("The server encountered an unexpected condition which prevented it from fulfilling the request.")
            print("Error Message: " + str(response.json()))
            print("If this persists, please contact customer care.")
        else:
            print("Error Message: " + str(response.json()))

        sys.exit()

    except Exception as err:
        print("An unexpected error occurred")
        print("Error Message: {0}".format(err))
        sys.exit()
        

def keywords_check(keywords:list, sentence:str):
    '''
    get the keywords in a sentence
    '''
    keywords = list(set(keywords)) # get the unique keywords
    keyword_list = [str(keyword).lower() for keyword in keywords] # make it lower case
    
    words_in_sentence = list(set([str(word).lower() for word in sentence.split()])) # get the unique words in a sentence in lower case

    keywords_in_sentence = []
    
    for keyword in keyword_list:
        if keyword in words_in_sentence:
            keywords_in_sentence.append(keyword)
    
    keywords_in_sentence = ','.join(keywords_in_sentence)
        
    return keywords_in_sentence
    

def unpivot_keywords(df:pd.DataFrame):
    '''
    unpivoting the keywords column
    '''
    df['keywords'] = df['keywords'].str.split(',')

    # Explode the 'keywords' column to create a new row for each individual word
    df_unpivoted = df.explode('keywords')

    # Reset the index if needed
    df_unpivoted = df_unpivoted.reset_index(drop=True)
    
    return df_unpivoted

def keyword_sort(keywordList:pd.DataFrame,keyword_col):
    '''
    key to sort the result dataframes
    keywordList: The keywords and their ranking
    keyword_col: the keyword column to be sorted
    '''
    keywordList['Keywords']=keywordList['Keywords'].apply(lambda x: str(x).lower()) # ensure the keywords are string 
    keyword_col = keyword_col.apply(str.lower)
    df = pd.DataFrame()
    df['keyword'] = keyword_col
    rank_col = df.merge(keywordList, how='left', left_on='keyword', right_on = 'Keywords')['ranking']
    rank_col = rank_col.fillna(10)

    return rank_col

def time_frame_translation(timeframe):
    match timeframe:
        case 'week':
            days = 7
        case 'day':
            days = 1
        case 'month':
            days = 31
        case 'quarter':
            days = 124
        case 'year':
            days = 365
        case 'all':
            days = 1000
        case _:
            if type(timeframe)== int:
                days =timeframe
            else:
                days = 1000
    return days
    

'''
def word_frequence(title_column):
    news_titles = [x for x in title_column]
    stop_words = set(stopwords.words('english'))
    punctuation = set(string.punctuation)

    word_freq = FreqDist()
    for title in news_titles:
        tokens = word_tokenize(title.lower())  # Convert to lowercase
        tokens = [word for word in tokens if word not in stop_words and word not in punctuation]  # Remove stopwords and punctuation
        word_freq.update(tokens)
    
    return word_freq.most_common()
'''