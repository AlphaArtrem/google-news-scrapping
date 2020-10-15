import urllib
import requests
from bs4 import BeautifulSoup
import time
import csv
from pathlib import Path
import os

# Search scrapper
def google_search():
    # Current woking directory
    current_path = os.getcwd()
    # List of queries
    es_entities = [
        "Nba",
        "UFC",
        "Mma",
        "Boxing",
        "Tennis",
        "F1",
        "Nfl",
        "Nascar",
        "Wwe",
        "Fortnite",
        "Call of duty",
        "Ps5",
        "Xbox",
        "Animal Crossing",
        "Apex legends",
    ]
    # Field for csv files
    fields = ['timestamp', 'keywords']
    for query in es_entities:
        # Remove spaces from queries
        query = query.replace(' ', '+')
        # Search url
        # tbm = nws means search news section
        # gl = us means reasult for us region
        URL = f"https://google.com/search?q={query}&tbm=nws&gl=us"
        # User agent to mask script as device
        USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:65.0) Gecko/20100101 Firefox/65.0"
        headers = {"user-agent" : USER_AGENT}
        # Sending request to google
        resp = requests.get(URL, headers=headers)

        # If the request was successfull
        if resp.status_code == 200:
            # Results for current query
            results = []
            # Data from existing csv
            existing_csv_rows = []
            # Path for current query's csv file
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
                            existing_csv_rows.append(row)
                    csvfile.close()
            # Parse data from google as html
            soup = BeautifulSoup(resp.content, "html.parser")
            # Find divs with class Vouh6c as it is used for people also searched sections
            for g in soup.find_all('div', class_ = 'Vouh6c'):
                # Get time stamp
                ts = time.gmtime()
                # If new csv file add result else only add if the result was not found in last 3 records
                if len(existing_csv_rows) == 0:
                    results.append([time.strftime("%Y-%m-%d %H:%M:%S", ts), g.text])
                else:
                    if len(existing_csv_rows) >= 3:
                        if g.text != existing_csv_rows[0][1] and g.text != existing_csv_rows[1][1] and g.text != existing_csv_rows[2][1]:
                            results.append([time.strftime("%Y-%m-%d %H:%M:%S", ts), g.text])
                    elif len(existing_csv_rows) == 2:
                        if g.text != existing_csv_rows[0][1] and g.text != existing_csv_rows[1][1]:
                            results.append([time.strftime("%Y-%m-%d %H:%M:%S", ts), g.text])
                    elif len(existing_csv_rows) == 1:
                        if g.text != existing_csv_rows[0][1]:
                            results.append([time.strftime("%Y-%m-%d %H:%M:%S", ts), g.text])
            # If csv file did exist add its data    
            if len(existing_csv_rows) > 0:
                results.extend(existing_csv_rows)
            # Add new result to csv
            with open(current_path + f"/csv/{query}.csv", 'w', newline = '') as csvfile: 
                csvwriter = csv.writer(csvfile) 
                csvwriter.writerow(fields)
                csvwriter.writerows(results)
                csvfile.close()
        else:
            print("Google deined request with error code : " + str(resp.status_code))

# Fire search scrapper every 10 mins or 60*10 seconds
while True:
    google_search()
    time.sleep(60 * 10)