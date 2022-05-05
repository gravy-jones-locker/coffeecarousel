import os
import pandas as pd

from typing import Any

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

SHEET_URI = '1JuuY4M67kWPk7d68Iqqew-NsprBK3bqxuLVl4YsrwTM'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

class SpreadSheet:
    """
    Interface for interacting with Google Sheet.
    """
    def __init__(self) -> None:
        """
        Establish connection to spreadsheet.
        """
        self._service = self._build_service()
    
    def get_data(self, sheet: str) -> pd.DataFrame:
        """
        Download data from the given sheet as a Pandas DataFrame object.
        :param sheet: the name of the sheet to download.
        :return: a Pandas DataFrame containing the sheet contents.
        """
        values = self._get_sheet_values(sheet)
        return pd.DataFrame.from_records(values[1:], columns=values[0])    
    
    def upload(self, sheet: str, data: pd.DataFrame) -> None:
        """
        Upload given data to the given sheet.
        :param sheet: the name of the sheet to upload to.
        :param data: the data to upload.
        """
        return self._service.spreadsheets().values().update(
            spreadsheetId=SHEET_URI,
            valueInputOption='RAW',
            range=self._get_sheet_range(sheet),
            body=self._get_upload_body(data)).execute()
    
    def _get_sheet_values(self, sheet: str) -> list:
        return self._service.spreadsheets().values().get(
            spreadsheetId=SHEET_URI,
            range=self._get_sheet_range(sheet)).execute().get('values', [])
    
    def _get_sheet_range(self, sheet: str) -> str:
        if os.environ["DEBUG_CAROUSEL"] == 'True':
            sheet = f'{sheet}_TEST'
        return sheet
    
    def _get_upload_body(self, data: pd.DataFrame) -> None:
        return {"values": [list(data.columns.values), 
                         *[list(x) for x in data.values]]}

    def _build_service(self) -> Any:
        creds = None
        if os.path.exists('settings/token.json'):
            creds = Credentials.from_authorized_user_file(
                'settings/token.json', SCOPES)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'settings/credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open('settings/token.json', 'w') as token:
                token.write(creds.to_json())
        return build('sheets', 'v4', credentials=creds)