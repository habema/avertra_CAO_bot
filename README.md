# Avertra Chief Anniversary Officer (CAO) Slack Bot
This is a "Slack Bot" that sends a message to a certain Slack channel with that day's birthdays and anniversaries of the employees of Avertra Corp.

## Requirements
This project assumes the existence of the following:
- A Google Spreadsheet with the following structure:
    | Employee Name | Birthday   | Hire Date  |
    |---------------|------------|------------|
    | John Doe      | 01/01/2000 | 01/01/2023 |
    | Jane Doe      | 12/25/1995 | 12/25/2024 |
> **Note:** The column names must be exactly as shown above. The dates must be in the format `MM/DD/YYYY`.
- `creds.json`: JSON file containing the creditionals for the Google Sheets API. The creditionals must belong to a services account that has access to the Google Spreadsheet. The file must have the following structure:
```json
{
  "type": "service_account",
  "project_id": "project-id",
  "private_key_id": "private-key-id",
  "private_key": "-----BEGIN PRIVATE KEY-----\nprivate-key\n-----END PRIVATE KEY-----\n",
  "client_email": "service-account-email",
  "client_id": "client-id",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/service-account-email",
  "universe_domain": "googleapis.com"
}
```
- `.env`: Environment variables file with the following structure:
```
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX
SPREADSHEET_NAME="Google Spreadsheet Name"
```
> **Note:** The `SLACK_WEBHOOK_URL` must be a valid [Slack Webhook URL](https://api.slack.com/messaging/webhooks). The `SPREADSHEET_NAME` must be the name of the Google Spreadsheet.

## How to run
1. Clone this repository
2. Install the dependencies with `pip install -r requirements.txt`
3. Run the script with `python main.py` to send the message to the Slack channel everyday at 8:00 AM Amman Time.

## To-Do
- [x] Built-in scheduling
- [ ] Edit GIFs to only be square