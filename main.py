import urllib
import requests
from bs4 import BeautifulSoup
import time
import csv
from pathlib import Path
import os

current_path = os.getcwd()
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
fields = ['timestamp', 'keywords']
for query in es_entities:
    query = query.replace(' ', '+')
    URL = f"https://google.com/search?q={query}&tbm=nws&gl=us"
    USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:65.0) Gecko/20100101 Firefox/65.0"
    headers = {"user-agent" : USER_AGENT}
    resp = requests.get(URL, headers=headers)

    if resp.status_code == 200:
        results = []
        existing_csv_rows = []
        existing_csv_path = Path(current_path + f"/csv/{query}.csv")
        if existing_csv_path.exists() :
            with open(current_path + f"/csv/{query}.csv", 'r') as csvfile: 
                csvreader = csv.reader(csvfile) 
                _ = next(csvreader)
                for row in csvreader: 
                    if row != []:
                        existing_csv_rows.append(row)
                csvfile.close()
        soup = BeautifulSoup(resp.content, "html.parser")
        for g in soup.find_all('div', class_ = 'Vouh6c'):
            ts = time.gmtime()
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
            
        if len(existing_csv_rows) > 0:
            results.extend(existing_csv_rows)
        with open(current_path + f"/csv/{query}.csv", 'w', newline = '') as csvfile: 
            csvwriter = csv.writer(csvfile) 
            csvwriter.writerow(fields)
            csvwriter.writerows(results)
            csvfile.close()
            
        