from .NewsCollection import NewsCollect
from .const import DATABASE_NAME, EMAIL_RECEIVERS
from .DataSharing import Email

def data_update() -> None:
    
    # get the new data and update the database
    geps_news = NewsCollect().geps_news
    yahoo_news = NewsCollect().yahoo_news
    rss_feeds_news =NewsCollect().rss_feeds_news
    google_news = NewsCollect().google_news
    
    print(  f'''
            Summary:
            GEPS News:      {len(geps_news)}
            Yahoo News:     {len(yahoo_news)}
            Rss News:       {len(rss_feeds_news)}
            Google News:    {len(google_news)}
            Data written into database: {DATABASE_NAME}
            ''')

def email_data(time_frame = 'week', attach_reports = False) -> None:
    # email the data
    mail = Email(time_frame)
    mail.email_data_with_gmail(attach_reports=attach_reports)
    print(  f'''
            Email sent to the following address:
            {EMAIL_RECEIVERS}
            through Gmail
            ''')