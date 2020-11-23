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

def telegram_bot_sendtext(bot_message, messageID, bot):
    if bot in telegram_creds.keys():
        bot_token = telegram_creds[bot]
        bot_chatID = messageID
        send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message

        response = requests.get(send_text)

        return response.json()

def get_user_subs(csvName):
    user_subs = {}
    with open(current_path + f"/csv/{csvName}.csv", 'r', encoding='windows-1252') as csvfile: 
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
def telegram_updates(query, ts, keyword, trending = 0):
    user_subs = get_user_subs("SuperUsers")
    specific_users = get_user_subs("SpecificUsers")
    if query in user_subs.keys():
        for user in user_subs[query]:
            if trending > 0:
                telegram_bot_sendtext(f"keyword : {keyword} for entity : {query} has been trending for {trending} hours", user, "super")
            else:
                telegram_bot_sendtext("entity : " + query + "\ntimestamp : " + ts + "\nkeyword : " +  keyword, user, "super")
    if query in specific_users.keys():
        for user in specific_users[query]:
            if trending > 0:
                telegram_bot_sendtext(f"keyword : {keyword} for entity : {query} has been trending for {trending} hours", user, query)
            else:
                telegram_bot_sendtext("entity : " + query + "\ntimestamp : " + ts + "\nkeyword : " +  keyword, user, query)

                
def telegram_top_updates(query, top_keywords):
    user_subs = get_user_subs("SuperUsers")
    specific_users = get_user_subs("SpecificUsers")
    message = f"Top keywords for today for {query}:"
    for keyword in top_keywords:
        message = message + "\n" + keyword[1] + " found " + keyword[0] + " times"
    if query in user_subs.keys():
        for user in user_subs[query]:
            telegram_bot_sendtext(message, user, "super")
    if query in specific_users.keys():
        for user in specific_users[query]:
            telegram_bot_sendtext(message, user, query)
