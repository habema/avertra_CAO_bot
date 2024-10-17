import os
import json
import random
import logging
import gspread
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials

from schedule import every, repeat, run_pending
import time

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler("bot_activity.log"),
        logging.StreamHandler()
    ]
)

logging.info('Bot started!')

BIRTHDAY_GIFS = json.load(open('bin/birthday_gifs.json'))
ANNIVERSARY_GIFS = json.load(open('bin/anniversary_gifs.json'))

def get_data(spreadsheet_name: str):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name('creds.json', scope)
    client = gspread.authorize(credentials)
    sheet = client.open(spreadsheet_name).sheet1
    data = sheet.get_all_records() 
    data = pd.DataFrame(data)
    return data

def get_birthdays(df: pd.DataFrame):
    df['Birthday'] = pd.to_datetime(df['Birthday'])
    today = datetime.now().date()
    today_month_day = today.strftime('%m-%d')
    birthday_today = df.loc[df['Birthday'].dt.strftime('%m-%d') == today_month_day, 'Employee Name'].tolist()
    return birthday_today

def get_anniversaries(df: pd.DataFrame):
    df['Hire Date'] = pd.to_datetime(df['Hire Date'])
    today = datetime.now().date()
    today_month_day = today.strftime('%m-%d')
    anniversary_today = df.loc[df['Hire Date'].dt.strftime('%m-%d') == today_month_day, 'Employee Name'].tolist()
    return anniversary_today

def parse_title():
    title = json.load(open('bin/title.json'))
    title['blocks'][1]['elements'][0]['text'] = title['blocks'][1]['elements'][0]['text'].replace('{{DATE}}', datetime.today().strftime('%B %d, %Y'))
    return title

def parse_birthday_header():
    birthday_header = json.load(open('bin/birthday_header.json'))
    birthday_header['accessory']['image_url'] = random.choice(BIRTHDAY_GIFS)
    return birthday_header

def parse_anniversary_header():
    anniversary_header = json.load(open('bin/anniversary_header.json'))
    anniversary_header['accessory']['image_url'] = random.choice(ANNIVERSARY_GIFS)
    return anniversary_header

def prepare_message():
    df = get_data(os.getenv('SPREADSHEET_NAME'))
    birthdays = get_birthdays(df)
    anniversaries = get_anniversaries(df)

    message = None
    if birthdays or anniversaries:
        message = parse_title()
        if birthdays:
            message['blocks'].append({
                "type": "divider"
            })
            message['blocks'].append(parse_birthday_header())
            message['blocks'].append({
                "type": "rich_text",
                "elements": []
            })
            for name in birthdays:
                message['blocks'][-1]['elements'].append({
                    "type": "rich_text_list",
                    "style": "bullet",
                    "elements": [
                        {
                            "type": "rich_text_section",
                            "elements": [
                                {
                                    "type": "text",
                                    "text": name.title()
                                }
                            ]
                        }
                    ]
                })

        if anniversaries:
            if birthdays:
                message['blocks'].append({
                    "type": "section",
                    "text": {
                        "type": "plain_text",
                        "text": "\n"
                    }
                })
            message['blocks'].append({
                "type": "divider"
            })
            message['blocks'].append(parse_anniversary_header())
            message['blocks'].append({
                "type": "rich_text",
                "elements": []
            })
            for name in anniversaries:
                message['blocks'][-1]['elements'].append({
                    "type": "rich_text_list",
                    "style": "bullet",
                    "elements": [
                        {
                            "type": "rich_text_section",
                            "elements": [
                                {
                                    "type": "text",
                                    "text": name.title()
                                }
                            ]
                        }
                    ]
                })
    
    return message

def send_message(message):
    if message:
        r = requests.post(
            url=os.getenv('SLACK_WEBHOOK_URL'),
            json=message
        )

        if r.status_code == 200:
            logging.info('Message sent successfully!')
        else:
            logging.error('Error sending message: %s, %s', r.status_code, r.text)

    else:
        logging.info('No birthdays or anniversaries today!')

@repeat(every().day.at('08:00', 'Asia/Amman'))
def main():
    message = prepare_message()
    send_message(message)


if __name__ == '__main__':
    while True:
        run_pending()
        time.sleep(1)