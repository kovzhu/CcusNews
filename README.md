# CcusNews

The package reads news from Yahoo Finance, Rss Feeds, Google news and prepare them as email based on keywords.

import datetime
import CcusNews

if datetime.datetime.now().weekday() == 4: # send email on Friday
    CcusNews.data_update()
    CcusNews.email_data(time_frame=7, attach_reports=True)

else:
    CcusNews.data_update()
    # CcusNews.email_data(time_frame=7, attach_reports=True)
