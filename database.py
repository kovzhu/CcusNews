import sqlite3
import pandas as pd 
import os
import datetime

from .const import DATABASE_NAME

class Database:
    
    def __init__(self) -> None:
        self._database_name = os.path.join(os.path.dirname(__file__),'data',DATABASE_NAME)
        
    
    def database_write(self, data:pd.DataFrame, table_name:str) -> None:
        '''
        data_names:
            rss_data
            yahoo_news_data
            geps_news
            google_news

        '''
        conn = sqlite3.connect(self._database_name)
        cursor = conn.cursor()
        
        # archive the rss feeds data -=====================================================================================================================
        # create the table if not exist
        
        match table_name:
            case 'rss_data':
                '''
                data columns:
                ['title', 'link', 'description', 'published']
                '''
                create_table_query = f'''
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        title TEXT,
                        link TEXT,
                        description TEXT,
                        date_published DATETIME,
                        keywords TEXT
                    );
                    '''
            case 'yahoo_news_data':
                '''
                    data columns:
                    ['uuid', 'title', 'publisher', 'link', 'providerPublishTime', 'type',
                    'thumbnail', 'relatedTickers', 'searched_ticker', 'exchange',
                    'keywords']
                '''
                create_table_query = f'''
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        uuid TEXT,
                        title TEXT,
                        publisher TEXT,
                        link TEXT,
                        date_published DATETIME,
                        type TEXT,
                        thumbnail TEXT,
                        relatedTickers TEXT,
                        searched_ticker TEXT,
                        exchange TEXT,
                        keywords TEXT
                    );
                    '''
            case 'geps_news':
                create_table_query = f'''
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        upstream_intelligence_id VARCHAR(255),
                        title VARCHAR(1000),
                        document_url VARCHAR(255),
                        date_published DATETIME,
                        original_published_date DATE,
                        snipped_text TEXT,
                        keywords TEXT);'''
            case 'google_news':
                create_table_query = f'''
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        title TEXT,
                        url TEXT,
                        date_published DATETIME,
                        keywords TEXT);
                '''
        cursor.execute(create_table_query)


        existing_data = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        new_data = pd.concat([existing_data,data])
        # new_data = new_data[~new_data.duplicated(keep='first')]
        new_data = new_data.drop_duplicates()

        new_data.to_sql(table_name, conn, if_exists="replace", index=False)
        print(f'Data updated in database for {table_name}')
        
        # commit the data update
        conn.commit()
        conn.close()
        
    def database_read(self, table_name: str, time_frame = 8) -> pd.DataFrame:
        '''
        table_name = 
            rss_data
            yahoo_news_data
            geps_news
            google_news
        To read the tables in the database
        '''
        end_datetime = datetime.datetime.now()
        start_datetime = end_datetime - datetime.timedelta(days=time_frame)
        start_datetime = pd.Timestamp(start_datetime, tz='UTC')  
        start_datetime =start_datetime.strftime("%Y-%m-%d %H:%M:%S") # convert to a string for filtering the dataframe ... don't know why only this works

        
        conn = sqlite3.connect(self._database_name)
        sql_line = f"SELECT * FROM {table_name} where date_published > '{start_datetime}'"
        print(f"SQL line: {sql_line} executed")
        data = pd.read_sql(sql_line, conn)
        conn.close
        
        print(f"{len(data)} rows of data read from database" )
            
        return data