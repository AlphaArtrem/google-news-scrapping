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

cred_file = open("cred.json",)
creds = json.load(cred_file)
cred_file.close()
redshift_creds = creds["redshift"]

# Search scrapper
def redshift_update():
    # Current woking directory
    current_path = os.getcwd()
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

    data = []


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
                        data.append({
                            "entity" : query,
                            "timestamp" : row[0],
                            "carousel_keyword": row[1]
                        })
                csvfile.close()
    df = pd.DataFrame(data)
    conn = create_engine(f'postgresql://{redshift_creds["username"]}:{redshift_creds["password"]}@{redshift_creds["url"]}/{redshift_creds["database"]}')
    df.to_sql('esanalytics', conn, index=False, if_exists='replace')


redshift_update()    

