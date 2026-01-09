import json
import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

GOOGLE_SHEET_ID = '15G9U072EJkvkuePmWWKgIvYwfTfvGOVMdqL6AIMUwVA'
SHEET_NAME = 'Sheet1'

try:
    creds_dict = json.load(open('google_credentials.json'))
    credentials = Credentials.from_service_account_info(
        creds_dict,
        scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
    )
    service = build('sheets', 'v4', credentials=credentials)
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=GOOGLE_SHEET_ID, range=SHEET_NAME).execute()
    values = result.get('values', [])
    
    print(f'✅ Connected to Google Sheets')
    print(f'📊 Total rows fetched: {len(values)}')
    if values:
        print(f'📋 Headers: {values[0]}')
        print(f'📈 Column count: {len(values[0])}')
        if len(values) > 1:
            print(f'📊 Data rows: {len(values) - 1}')
            print(f'🔍 First row: {values[1]}')
    else:
        print('❌ No data in sheet')
        
except Exception as e:
    print(f'❌ Error: {e}')
    import traceback
    traceback.print_exc()
