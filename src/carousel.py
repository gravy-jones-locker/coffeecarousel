import pandas as pd
import smtplib

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr

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
        #if os.environ.get('DEBUG_CAROUSEL') != 'True':
        self.send_notifications()
        print('All done!')
    
    def get_data(self) -> None:
        """
        Retrieve and store subscriber/historical pair data.
        """
        print('Retrieving data')
        self.subs_df = self.spreadsheet.get_data('subscribers')
        self.subs_df = self.subs_df.drop(['Timestamp'], axis=1)
        self.subs_df.columns = ['name', 'email', 'days', 'exclusive']
        self.hist_df = self.spreadsheet.get_data('history')
        print('Data retrieved')
    
    def configure_subs(self) -> None:
        """
        Build Subscriber objects into a list from data.
        """
        self.subscribers = []
        #week_no = dt.utcnow().isocalendar()[1]
        for _, sub in self.subs_df.iterrows():
            sub = Subscriber(sub, self.hist_df)
            # Interval selection no longer implemented
            #if not sub._is_involved(week_no):
                #continue
            self.subscribers.append(sub)
        self.subscribers.sort(key=lambda x: x._total_matches())
    
    def match_pairs(self):
        """
        Intelligently match subscribers into pairs of emails.
        """
        unmatched = self.subscribers.copy()
        while len(unmatched) > 1:
            sub = unmatched.pop(0)
            if sub.partner is not None:
                continue
            sub.match(unmatched)
            if sub.partner is not None:
                unmatched.remove(sub.partner)

    def update_history(self) -> None:
        """
        Update historical data and upload into sheet.
        """
        print('Updating historical data')
        for sub in self.subscribers:
            if sub.partner is None:
                continue
            ad_row = sub.create_row(sub.partner)
            self.hist_df = pd.concat([self.hist_df, ad_row])
        self.spreadsheet.upload('history', self.hist_df)
    
    def send_notifications(self) -> None:
        """
        Send notifications to each member of a pair.
        """
        for sub in self.subscribers:
            self.send_email(sub)
    
    def send_email(self, sub: Subscriber) -> None:
        """
        Send notification email to the given subscriber.
        """
        print(f'sending email to {sub._data["email"]}')
        body = f"""Hi!

You have been matched with {sub.partner._data["name"]} for this week's coffee carousel.

Drop them a line at {sub.partner._data["email"]} to sort something out. 

Till next time,

The Union St Coffee Carousel
"""
        msg = MIMEMultipart('alternative')

        user = 'unionstcoffeecarousel@outlook.com'
        password = 'QGXWCPfSz858u7p'
        
        msg['Subject'] = 'Coffee Time'
        msg['From'] = formataddr(('Union St Coffee Carousel', user))
        msg['To'] = formataddr((sub._data["name"], sub._data["email"]))

        content = MIMEText(body, "plain")
        msg.attach(content)

        session = smtplib.SMTP('smtp-mail.outlook.com', 587)
        session.starttls()
        session.login(user, password)

        session.sendmail(user, [sub._data["email"]], msg.as_string())
        session.close()