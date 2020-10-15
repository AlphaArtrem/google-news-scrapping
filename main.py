import urllib
import requests
from bs4 import BeautifulSoup
import time
import csv
from pathlib import Path

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
field = ['timestamp', 'keywords']
all_queries = []
for query in es_entities:
    query = query.replace(' ', '+')
    URL = f"https://google.com/search?q={query}&tbm=nws&gl=us"

    USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:65.0) Gecko/20100101 Firefox/65.0"

    headers = {"user-agent" : USER_AGENT}
    resp = requests.get(URL, headers=headers)

    if resp.status_code == 200:
        soup = BeautifulSoup(resp.content, "html.parser")
        results = []
        for g in soup.find_all('div', class_='Vouh6c'):
            ts = time.gmtime()
            results.append({time.strftime("%Y-%m-%d %H:%M:%S", ts) : g.text})
        all_queries.append(query, results])
print(all_queries)