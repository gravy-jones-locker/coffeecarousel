import os
import pandas as pd
import smtplib

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime as dt
from concurrent.futures import ThreadPoolExecutor

from src.spreadsheet import SpreadSheet
from src.subscriber import Subscriber

class Carousel:
    """
    Spin the carousel to generate coffee partners.
    """
    def __init__(self):
        self.spreadsheet = SpreadSheet()
    
    def spin(self) -> None:
        """
        Sequences matching/notification flow.
        """
        print('Spinning the carousel..')
        self.get_data()
        self.configure_subs()
        self.match_pairs()
        self.update_history()
        if os.environ.get('DEBUG_CAROUSEL') != 'True':
            self.send_notifications()
        print('All done!')
    
    def get_data(self) -> None:
        """
        Retrieve and store subscriber/historical pair data.
        """
        print('Retrieving data')
        self.subs_df = self.spreadsheet.get_data('subscribers')
        self.subs_df = self.subs_df.drop(['Timestamp'], axis=1)
        self.subs_df.columns = ['name', 'email', 'interval']
        self.hist_df = self.spreadsheet.get_data('history')
        print('Data retrieved')
    
    def configure_subs(self) -> None:
        """
        Build Subscriber objects into a list from data.
        """
        self.subscribers = []
        week_no = dt.utcnow().isocalendar()[1]
        for _, sub in self.subs_df.iterrows():
            sub = Subscriber(sub, self.hist_df)
            if not sub._is_involved(week_no):
                continue
            self.subscribers.append(sub)
        self.subscribers.sort(key=lambda x: x._total_matches())
    
    def match_pairs(self):
        """
        Intelligently match subscribers into pairs of emails.
        """
        unmatched = self.subscribers.copy()
        while len(unmatched) > 1:
            sub1 = unmatched.pop(0)
            if sub1.partner is not None:
                continue
            sub1.match(unmatched)
            unmatched.remove(sub1.partner)

    def update_history(self) -> None:
        """
        Update historical data and upload into sheet.
        """
        print('Updating historical data')
        for sub in self.subscribers:
            if sub.partner is None:
                continue
            self.update_history_row(sub)
        self.hist_df = self.hist_df.sort_values('email1')
        self.spreadsheet.upload('history', self.hist_df)
    
    def update_history_row(self, sub: Subscriber) -> None:
        """
        Update/insert a historical row given some match.
        """
        pair = sub._get_pair(sub.partner)
        if pair is None:
            pair = sub._create_pair(sub.partner)
            self.hist_df = pd.concat([self.hist_df, pair])
        else:
            self.hist_df.loc[pair.index, "count"] = str(int(pair["count"]) + 1)
    
    def send_notifications(self) -> None:
        """
        Send notifications to each member of a pair.
        """
        with ThreadPoolExecutor(5) as exc:
            for sub in self.subscribers:
                exc.submit(self.send_email, sub)
    
    def send_email(self, sub: Subscriber) -> None:
        """
        Send notification email to the given subscriber.
        """
        print(f'sending email to {sub._data["email"]}')
        body = f"""
Hi!

You have been matched with {sub.partner._data["name"]} for this week's coffee carousel.

Drop them a line at {sub.partner._data["email"]} to sort something out. 

Till next time,

The Union St Coffee Carousel
"""
        msg = MIMEMultipart('alternative')
        
        msg['Subject'] = 'Coffee Time'
        msg['From'] = 'Union St Coffee Carousel'
        msg['To'] = sub._data["name"]

        content = MIMEText(body, "plain")
        msg.attach(content)

        user = 'unionstcoffeecarousel@gmail.com'
        password = 'yjdtaxysrqypling'

        session = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        session.login(user, password)

        session.sendmail(user, [sub._data["email"]], msg.as_string())
        session.close()