import urllib
import requests
from bs4 import BeautifulSoup
import time
import csv
from pathlib import Path
import os
from sqlalchemy import create_engine
import pandas as pd
import json

# Current woking directory
current_path = os.getcwd()
cred_file = open("cred.json",)
creds = json.load(cred_file)
cred_file.close()
telegram_creds = creds["telegram"]

def telegram_bot_sendtext(bot_message, username):
    bot_token = telegram_creds["bot_id"]
    bot_chatID = '1283355929'
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message

    response = requests.get(send_text)

    return response.json()

def get_user_subs():
    user_subs = {}
    with open(current_path + f"/csv/users.csv", 'r') as csvfile: 
            csvreader = csv.reader(csvfile) 
            # Ignore csv headers
            _ = next(csvreader)
            # Read each row
            for row in csvreader: 
                # Only append a row if it is not empty as csv writer makes last row empty
                if row != []:
                    subs = []
                    for sub in row[1:len(row)]:
                        if sub != "":
                           subs.append(sub)
                    user_subs[row[0]] = subs
            csvfile.close()
    return user_subs

# Search scrapper
def telegram_updates():
    # List of queries
    es_entities = [
        "NBA",
        "UFC",
        "MMA",
        "Boxing",
        "Tennis",
        "F1",
        "NFL",
        "Nascar",
        "WWE",
        "Fortnite",
        "Call of duty",
        "PS5",
        "Xbox",
        "Animal Crossing",
        "Apex legends",
    ]

    user_subs = get_user_subs()
    

    # Field for csv files
    fields = ['timestamp', 'keywords']
    for query in es_entities[0:1]:
        # Remove spaces from queries
        query = query.replace(' ', '+')
        existing_csv_path = Path(current_path + f"/csv/{query}.csv")
        # Read data if csv exists
        if existing_csv_path.exists() :
            with open(current_path + f"/csv/{query}.csv", 'r') as csvfile: 
                csvreader = csv.reader(csvfile) 
                # Ignore csv headers
                _ = next(csvreader)
                # Read each row
                for row in csvreader: 
                    # Only append a row if it is not empty as csv writer makes last row empty
                    if row != []:
                        for user in user_subs[query]:
                            telegram_bot_sendtext("entity : " + query + "\ntimestamp : " + row[0] + "\nkeyword : " +  row[1], user)
                csvfile.close()
    
# Fire search scrapper every 10 mins or 60*10 seconds
while True:
    telegram_updates()
    time.sleep(60 * 2)