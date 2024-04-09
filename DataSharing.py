from Google import Create_Service
import pandas as pd

import os
import io
import datetime

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import base64

from .database import Database
from .utils import keyword_sort, time_frame_translation
from .const import EMAIL_RECEIVERS
from .NewsSource.GepsNews import GepsNews

timestamp = datetime.datetime.strftime(datetime.datetime.now(),'%d-%b-%Y') # can be used in file name

class Email:
    '''
    send emails with the data in the database
    time_frame: 
        week;
        day;
        month;
        quarter;
        year;
        all;
        integer number
    
    '''
    def __init__(self, time_frame = 'week', email_receivers = EMAIL_RECEIVERS) -> None:
        self._time_frame = time_frame_translation(time_frame)
        self.rss_data_for_email = Database().database_read('rss_data', self._time_frame)
        self.yh_data_for_email = Database().database_read('yahoo_news_data',self._time_frame).drop(['uuid','type', 'thumbnail'], axis=1) # drop the columns not needed in email
        self.geps_data_for_email = Database().database_read('geps_news', self._time_frame)[['title', 'document_url',  'date_published', 'snipped_text']]
        self.google_news_for_email = Database().database_read('google_news', self._time_frame)
        
        self._keywords = pd.read_excel(os.path.join(os.path.dirname(__file__),'data','CCUS sources and keywords.xlsx'), sheet_name='Keywords')
        self._email_subject = f'Weekly CCUS news update on {timestamp}'
        
        self._email_receivers = email_receivers
        
    def _email_preparation(self, save_locally =False):
        '''
        Prepare the email body 
        '''
        
        # remove the news that do not match any keywords ==========================================================================
        rss_data_for_email = self.rss_data_for_email.drop_duplicates(subset=['title']) # remove the news that does not match the keywords
        yh_data_for_email = self.yh_data_for_email.drop_duplicates(subset=['title'])
        geps_data_for_email = self.geps_data_for_email.drop_duplicates(subset=['title'])
        google_news_for_email = self.google_news_for_email.drop_duplicates(subset=['title'])
        
        rss_data_for_email['keywords'].replace('', pd.NA, inplace=True)
        yh_data_for_email['keywords'].replace('', pd.NA, inplace=True)
        google_news_for_email['keywords'].replace('', pd.NA, inplace=True)
        # geps_data_for_email['keywords'].replace('', pd.NA, inplace=True)
        
        rss_data_for_email.dropna(subset=['keywords'], inplace=True)
        yh_data_for_email.dropna(subset=['keywords'], inplace=True)
        google_news_for_email.dropna(subset=['keywords'], inplace=True)
        # geps_data_for_email.dropna(subset=['keywords'], inplace=True) # all geps news are needed

        # rank the news based on relevancy ========================================================================================

        rss_data_for_email = rss_data_for_email.sort_values(by='keywords',key=lambda x:keyword_sort(self._keywords,x))
        yh_data_for_email = yh_data_for_email.sort_values(by='keywords',key=lambda x:keyword_sort(self._keywords,x))
        google_news_for_email = google_news_for_email.sort_values(by='keywords',key=lambda x:keyword_sort(self._keywords,x))

        # styles = {
        #     'table': 'border-collapse: collapse; width: 100%;',
        #     'th': 'background-color: #f2f2f2; border: 1px solid black; padding: 8px; text-align: left;',
        #     'td': 'border: 1px solid black; padding: 8px; text-align: left;'
        #         }

        # Define inline CSS styles for the table =========================================================================
        css_styles = """
        <style>
        table {
            border-collapse: collapse;
            width: 100%;
        }
        th, td {
            border: 1px solid black;
            padding: 8px;
            text-align: left;
        }
        th {
            background-color: #f2f2f2;
        }
        </style>
        """

        # create the html message body ==========================================================================================
        html_rss = css_styles + rss_data_for_email.to_html(index=False)
        html_yh = css_styles + yh_data_for_email.to_html(index=False)
        html_geps = css_styles + geps_data_for_email.to_html(index=False)
        html_google = css_styles + google_news_for_email.to_html(index=False)

        message_body = f"""
        <html>
        <head>
            <link rel="stylesheet" type="text/css" href="styles.css">
        </head>
        <body>
            <h3 style="background: rgb(238, 238, 238); border: 1px solid rgb(204, 204, 204); padding: 5px 10px;"><strong>GEPS CCUS news on {timestamp}</strong><o:p></o:p></h3>
            {html_geps}
        </body>
        <body>
            <h3 style="background: rgb(238, 238, 238); border: 1px solid rgb(204, 204, 204); padding: 5px 10px;"><strong>Yahoo news on {timestamp}</strong><o:p></o:p></h3>
            {html_yh}
        </body>
        <body>
            <h3 style="background: rgb(238, 238, 238); border: 1px solid rgb(204, 204, 204); padding: 5px 10px;"><strong>RSS news on {timestamp}</strong><o:p></o:p></h3>
            {html_rss}
        </body>
        <body>
            <h3 style="background: rgb(238, 238, 238); border: 1px solid rgb(204, 204, 204); padding: 5px 10px;"><strong>Google news on {timestamp}</strong><o:p></o:p></h3>
            {html_google}
        </body>
        </html>
        """
        # save the data as Excel to be sent as attachement =================================================================================

        # remove timezone information, as datatime with timezone info cannot be written in Excel file
        pd.to_datetime(rss_data_for_email.date_published).dt.tz_localize(None)
        yh_data_for_email.date_published = pd.to_datetime(yh_data_for_email.date_published).dt.tz_localize(None)
        geps_data_for_email.date_published = pd.to_datetime(geps_data_for_email.date_published).dt.tz_localize(None)
        google_news_for_email.date_published = pd.to_datetime(google_news_for_email.date_published).dt.tz_localize(None)
        
        # save the data as excel
        if save_locally == True:
            with pd.ExcelWriter(f"CCUS news folder/{self._email_subject}.xlsx", engine='xlsxwriter') as writer:
                rss_data_for_email.to_excel(writer, sheet_name='RSS news', index=False)
                yh_data_for_email.to_excel(writer, sheet_name='Yahoo news', index=False)
                geps_data_for_email.to_excel(writer, sheet_name='GEPS news', index=False)
                google_news_for_email.to_excel(writer, sheet_name='Google news', index=False)

        # prepare the email content =========================================================
        # prepare the excel file
        excel_data = io.BytesIO()
        with pd.ExcelWriter(excel_data, engine='xlsxwriter') as writer:
            rss_data_for_email.to_excel(writer, sheet_name='RSS news', index=False)
            yh_data_for_email.to_excel(writer, sheet_name='Yahoo news', index=False)
            geps_data_for_email.to_excel(writer, sheet_name='GEPS news', index=False)
            google_news_for_email.to_excel(writer, sheet_name='Google news', index=False)
        excel_data.seek(0)

        excel_filename = f'Weekly CCUS data at {timestamp}.xlsx'
        email_content = MIMEBase('application', 'vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        email_content.set_payload(excel_data.read())
        encoders.encode_base64(email_content)
        email_content.add_header('Content-Disposition', f'attachment; filename={excel_filename}')
        
        print(f'''
                Summary of data for email:
                GEPS news to send: {len(geps_data_for_email)}
                Yahoo news to send: {len(yh_data_for_email)}
                RSS news to send: {len(rss_data_for_email)}
                Google news to send: {len(google_news_for_email)}
                ''')

        return message_body, email_content
    
    def email_data_with_gmail(self, attach_reports = False):

        '''
        Send the results as email
        v2 update: Add Excel attachment in the email
        '''

        # CLIENT_SECRET_FILE = os.path.join(os.getcwd(),'client_secret.json')
        CLIENT_SECRET_FILE = os.path.join(os.getcwd(),'data','client_secret_UTS.json')
        API_NAME = 'gmail'
        # API_NAME = 'News monitoring'
        API_VERSION = 'v1'
        SCOPES = ['https://mail.google.com/']

        # EMAIL_ADDRESS = 'kunfengzhu@gmail.com'
        EMAIL_ADDRESS = 'UpstreamTransformationService@gmail.com'
        TO_ADDRESS = self._email_receivers

        service = Create_Service(CLIENT_SECRET_FILE, API_NAME, API_VERSION, SCOPES)

        msg = MIMEMultipart('mixed')
        msg['Subject'] = self._email_subject
        msg['From'] = EMAIL_ADDRESS
        msg['To'] = ','.join(TO_ADDRESS)

        message_body,email_body = self._email_preparation()

        # text_body = MIMEText("The weekly CCUS news update", "plain")
        # msg.attach(text_body)

        # attach the html email body
        html_body = MIMEText(message_body,'html')
        msg.attach(html_body)
        # attach the data as excel attachment
        msg.attach(email_body)
        # excel_data = io.BytesIO()
        # with pd.ExcelWriter(excel_data, engine='xlsxwriter') as writer:
        #     rss_data_for_email.to_excel(writer, sheet_name='RSS news', index=False)
        #     yh_data_for_email.to_excel(writer, sheet_name='Yahoo news', index=False)
        #     geps_data_for_email.to_excel(writer, sheet_name='GEPS news', index=False)
        # excel_data.seek(0)

        # excel_filename = f'Weekly CCUS data at {timestamp}.xlsx'
        # self.excel_part = MIMEBase('application', 'vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        # self.excel_part.set_payload(excel_data.read())
        # encoders.encode_base64(self.excel_part)
        # self.excel_part.add_header('Content-Disposition', f'attachment; filename={excel_filename}')


        # attach the geps pdf reports, when sending email  ====================================================
        if attach_reports is True:
            
            # get the pdf reports based on the time frame
            geps_pdfs = GepsNews(self._time_frame).GepsPdfReports
            
            for idx, pdf in enumerate(geps_pdfs):
                # # # pdf_data = io.BytesIO()
                # with open('temp.pdf', "wb") as pdf_file:
                #     pdf_file.write(pdf.content)
                pdf_filename = f"GEPS report {str(idx+1)}.pdf"
                # pdf_encoded = base64.urlsafe_b64encode(pdf.content).decode("utf-8")
                pdf_attachment = MIMEBase('application', 'pdf')
                # with open('temp.pdf','rb') as pdf_file:
                #     pdf_attachment.set_payload(pdf_file.read())
                pdf_attachment.set_payload(pdf.content)
                encoders.encode_base64(pdf_attachment)
                
                # pdf_attachment.set_payload(pdf.content)
                pdf_attachment.add_header('Content-Disposition', f'attachment; filename="{pdf_filename}"')
                msg.attach(pdf_attachment)

        raw_message = base64.urlsafe_b64encode(msg.as_bytes()).decode('utf-8')

        try:
            message = service.users().messages().send(userId='me', body={'raw':raw_message}).execute()
            print(message)
            print('Email sent successfully')
        except Exception as e:
            print(f"An error occured; email not sent: {e}")