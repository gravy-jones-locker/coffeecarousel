import pandas as pd
import os

from typing import Tuple, List
from datetime import datetime as dt
from pandas import DataFrame as Df

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
    
    def send_notifications(self, new_pairs: Df) -> None:
        """
        Send notifications to each member of a pair.
        """
        print('Sending notifications')