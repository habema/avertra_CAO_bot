"""
Birthday and Anniversary Slack Notification Bot with Enhanced Reliability, Security, and Centralized JSON Logging

This script automates Slack notifications for employee birthdays and work anniversaries.
It retrieves data from a Google Sheets document in read-only mode, checks if there are any birthdays or anniversaries
on the current date, and posts a custom message to a Slack webhook.

Enhancements:
- Secure environment variable management.
- Content validation to prevent SPAM and malicious links.
- Network efficiency with session and retry handling.
- Circuit breaker pattern for Slack requests to enhance reliability.
- Read-only permissions enforced for Google Sheets access.
- Structured JSON logging for centralized monitoring and error tracking.

Requirements:
- Google Sheets API credentials (`creds.json`).
- Environment variables stored in a `.env` file with the Slack webhook URL and spreadsheet name.

"""

import os
import json
import random
import logging
import gspread
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials
from schedule import every, repeat, run_pending
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Load environment variables from the .env file
load_dotenv()

# Configure structured JSON logging to record bot activity and errors
class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "message": record.getMessage(),
            "name": record.name,
            "module": record.module,
            "line": record.lineno,
        }
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_record)

json_handler = logging.StreamHandler()
json_handler.setFormatter(JSONFormatter())
logging.basicConfig(level=logging.INFO, handlers=[json_handler])

logging.info('Bot started!')

# Helper function to safely retrieve environment variables and log if they are missing
def get_env_variable(var_name):
    value = os.getenv(var_name)
    if value is None:
        logging.error(json.dumps({
            "error": "Environment variable missing",
            "variable": var_name,
        }))
    return value

# Load essential environment variables, logging any missing ones
SPREADSHEET_NAME = get_env_variable('SPREADSHEET_NAME')
SLACK_WEBHOOK_URL = get_env_variable('SLACK_WEBHOOK_URL')

# Helper function to load JSON files with error handling to avoid script failure if files are missing or malformed
def load_json_file(filepath: str):
    try:
        with open(filepath, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        logging.error(json.dumps({"error": "File not found", "filepath": filepath}))
    except json.JSONDecodeError:
        logging.error(json.dumps({"error": "JSON decoding error", "filepath": filepath}))
    return None

# Load GIF data for birthdays and anniversaries from JSON files, logging errors if files are missing
BIRTHDAY_GIFS = load_json_file('bin/birthday_gifs.json') or []
ANNIVERSARY_GIFS = load_json_file('bin/anniversary_gifs.json') or []

# Initialize a requests session with retry and backoff logic for Slack API requests
session = requests.Session()
retries = Retry(
    total=3,
    backoff_factor=2,
    status_forcelist=[500, 502, 503, 504],
)
session.mount("https://", HTTPAdapter(max_retries=retries))

# Circuit breaker pattern implementation to prevent overloading Slack with repeated requests during downtime
class CircuitBreaker:
    def __init__(self, threshold=5, cooldown=300):
        self.failure_count = 0
        self.threshold = threshold
        self.cooldown = cooldown
        self.last_failure_time = None

    def is_open(self):
        if self.failure_count >= self.threshold:
            if self.last_failure_time and (datetime.now() - self.last_failure_time).total_seconds() < self.cooldown:
                return True
            else:
                self.failure_count = 0  # Reset after cooldown
                return False
        return False

    def record_failure(self):
        self.failure_count += 1
        self.last_failure_time = datetime.now()

circuit_breaker = CircuitBreaker()

# Function to load Google Sheets credentials with read-only permissions
def load_credentials():
    """
    Loads Google Sheets API credentials with read-only access.
    Ensures the script cannot modify any data in the spreadsheet.
    """
    try:
        # Use read-only scope for Google Sheets to restrict permissions
        scope = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        credentials = ServiceAccountCredentials.from_json_keyfile_name('creds.json', scope)
        client = gspread.authorize(credentials)
        logging.info(json.dumps({"message": "Google Sheets credentials loaded successfully with read-only access."}))
        return client
    except FileNotFoundError:
        logging.error(json.dumps({"error": "Credentials file not found", "file": "creds.json"}))
    except Exception as e:
        logging.error(json.dumps({"error": "Failed to load Google Sheets credentials", "exception": str(e)}))
    return None

# Function to fetch data from a Google Sheets spreadsheet, returning a DataFrame or empty DataFrame if unavailable
def get_data(spreadsheet_name: str):
    client = load_credentials()
    if not client:
        return pd.DataFrame()  # Return an empty DataFrame if credentials are missing

    try:
        sheet = client.open(spreadsheet_name).sheet1
        data = sheet.get_all_records()
        return pd.DataFrame(data)
    except Exception as e:
        logging.error(json.dumps({
            "error": "Error fetching data from spreadsheet",
            "spreadsheet_name": spreadsheet_name,
            "exception": str(e)
        }))
        return pd.DataFrame()  # Handle missing or inaccessible data safely

# Function to validate and parse date columns in the DataFrame, logging any missing columns or parsing issues
def validate_date_column(df: pd.DataFrame, column_name: str):
    if column_name not in df.columns:
        logging.warning(json.dumps({"warning": "Missing column in data", "column": column_name}))
        return pd.Series(dtype='datetime64[ns]')

    try:
        return pd.to_datetime(df[column_name], errors='coerce')  # Coerce invalid dates to NaT
    except Exception as e:
        logging.error(json.dumps({
            "error": "Error parsing dates",
            "column_name": column_name,
            "exception": str(e)
        }))
        return pd.Series(dtype='datetime64[ns]')

# Function to get a list of employee names with birthdays today by matching month and day
def get_birthdays(df: pd.DataFrame):
    print(df)
    df['Birthday'] = validate_date_column(df, 'Birthday')
    today_month_day = datetime.now().strftime('%m-%d')
    birthdays_today = df.loc[df['Birthday'].dt.strftime('%m-%d') == today_month_day, 'Employee Name'].dropna().tolist()
    return birthdays_today

# Function to get a list of employee names with anniversaries today by matching month and day
def get_anniversaries(df: pd.DataFrame):
    df['Hire Date'] = validate_date_column(df, 'Hire Date')
    today_month_day = datetime.now().strftime('%m-%d')
    anniversaries_today = df.loc[df['Hire Date'].dt.strftime('%m-%d') == today_month_day, 'Employee Name'].dropna().tolist()
    return anniversaries_today

# Function to parse the title for the Slack message, replacing the date placeholder with the current date
def parse_title():
    title = load_json_file('bin/title.json')
    if title:
        title['blocks'][1]['elements'][0]['text'] = title['blocks'][1]['elements'][0]['text'].replace('{{DATE}}', datetime.today().strftime('%B %d, %Y'))
    return title

# Function to parse and update the birthday header with a randomly selected birthday GIF
def parse_birthday_header():
    birthday_header = load_json_file('bin/birthday_header.json')
    if birthday_header:
        birthday_header['accessory']['image_url'] = random.choice(BIRTHDAY_GIFS) if BIRTHDAY_GIFS else ""
    return birthday_header

# Function to parse and update the anniversary header with a randomly selected anniversary GIF
def parse_anniversary_header():
    anniversary_header = load_json_file('bin/anniversary_header.json')
    if anniversary_header:
        anniversary_header['accessory']['image_url'] = random.choice(ANNIVERSARY_GIFS) if ANNIVERSARY_GIFS else ""
    return anniversary_header

# Function to prepare the complete Slack message, including titles, birthday, and anniversary sections
def prepare_message():
    df = get_data(SPREADSHEET_NAME)
    birthdays = get_birthdays(df)
    anniversaries = get_anniversaries(df)

    message = None
    if birthdays or anniversaries:
        # Start with the main title of the message
        message = parse_title()
        
        # Add birthday section if there are birthdays today
        if birthdays:
            message['blocks'].append({"type": "divider"})
            message['blocks'].append(parse_birthday_header())
            message['blocks'].append({"type": "rich_text", "elements": []})
            for name in birthdays:
                message['blocks'][-1]['elements'].append({
                    "type": "rich_text_list",
                    "style": "bullet",
                    "elements": [{"type": "rich_text_section", "elements": [{"type": "text", "text": name.title()}]}]
                })

        # Add anniversary section if there are anniversaries today
        if anniversaries:
            # Add space between birthday and anniversary sections if both exist
            if birthdays:
                message['blocks'].append({"type": "section", "text": {"type": "plain_text", "text": "\n"}})
            message['blocks'].append({"type": "divider"})
            message['blocks'].append(parse_anniversary_header())
            message['blocks'].append({"type": "rich_text", "elements": []})
            for name in anniversaries:
                message['blocks'][-1]['elements'].append({
                    "type": "rich_text_list",
                    "style": "bullet",
                    "elements": [{"type": "rich_text_section", "elements": [{"type": "text", "text": name.title()}]}]
                })
    
    return message

# Function to validate message content for length and malicious links before sending to Slack
def validate_message_content(message):
    if not message:
        return False
    max_length = 500  # Maximum message length to prevent abuse

    # Check for any URLs or potentially harmful content in the message
    def contains_url(text):
        return any(keyword in text for keyword in ["http://", "https://", "www."])

    for block in message.get('blocks', []):
        for element in block.get('elements', []):
            text = element.get('text', '')
            if len(text) > max_length:
                logging.warning(json.dumps({"warning": "Message content exceeds max length"}))
                return False
            if contains_url(text):
                logging.warning(json.dumps({"warning": "Message contains prohibited URL"}))
                return False

    return True

# Function to send message to Slack after validation, with circuit breaker and improved error handling
def send_message(message):
    if circuit_breaker.is_open():
        logging.error(json.dumps({"error": "Circuit breaker open, skipping message"}))
        return
    
    if message and validate_message_content(message):
        try:
            response = session.post(url=SLACK_WEBHOOK_URL, json=message)
            response.raise_for_status()
            logging.info(json.dumps({"message": "Message sent successfully"}))
            circuit_breaker.failure_count = 0  # Reset on success
        except requests.exceptions.HTTPError as http_err:
            logging.error(json.dumps({
                "error": "HTTP error sending message",
                "status": response.status_code,
                "response": response.text,
                "exception": str(http_err)
            }))
            circuit_breaker.record_failure()
        except requests.exceptions.RequestException as req_err:
            logging.error(json.dumps({"error": "Network error", "exception": str(req_err)}))
            circuit_breaker.record_failure()
    else:
        logging.info(json.dumps({"info": "No birthdays/anniversaries today or validation failed"}))

# Main function that schedules the daily message check and send operation
@repeat(every().day.at('08:00', 'Asia/Amman'))
def main():
    message = prepare_message()
    send_message(message)

# Continuously run the scheduled task
if __name__ == '__main__':
    while True:
        run_pending()
        time.sleep(1)
