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
from static import es_entities
import psycopg2


cred_file = open("cred.json",)
creds = json.load(cred_file)
cred_file.close()
redshift_creds = creds["redshift"]

# Search scrapper
def redshift_update():
    # Current woking directory
    current_path = os.getcwd()
    data = []
    # Field for csv files
    fields = []
    for query in es_entities:
        # Remove spaces from queries
        query = query.replace(' ', '+')
        existing_csv_path = Path(current_path + f"/csv/{query}TopArticles.csv")
        if existing_csv_path.exists() :
            with open(current_path + f"/csv/{query}TopArticles.csv", 'r', newline = '') as csvfile: 
                csvreader = csv.reader(csvfile) 
                # Ignore csv headers
                fields = next(csvreader)
                # Read each row
                for row in csvreader: 
                    # Only append a row if it is not empty as csv writer makes last row empty
                    if row != []:
                        data.append({
                            fields[0] : row[0],
                            fields[1] : row[1],
                            fields[2]: row[2],
                            fields[3] : row[3],
                            fields[4] : row[4],
                            fields[5]: row[5],
                            fields[6] : row[6],
                            fields[7] : row[7],
                            fields[8]: row[8],
                        })
                csvfile.close() 
    df = pd.DataFrame(data)
    conn = create_engine(f'postgresql://{redshift_creds["username"]}:{redshift_creds["password"]}@{redshift_creds["host"]}:{redshift_creds["port"]}/{redshift_creds["database"]}')
    df.to_sql('esanalytics_google', conn, index=False, if_exists='replace')

con=psycopg2.connect(dbname= redshift_creds["database"], host=redshift_creds["host"], 
port= redshift_creds["port"], user= redshift_creds["username"], password= redshift_creds["password"])
cur = con.cursor()
current_path = os.getcwd()
existing_csv_path = Path(current_path + f"/csv/NBATopArticles.csv")
data = ()
if existing_csv_path.exists() :
    with open(current_path + f"/csv/NBATopArticles.csv", 'r', newline = '') as csvfile: 
        csvreader = csv.reader(csvfile) 
        # Ignore csv headers
        fields = next(csvreader)
        # Read each row
        for row in csvreader: 
            # Only append a row if it is not empty as csv writer makes last row empty
            if row != []:
                data.append(tuple(row))
        csvfile.close() 
    args_str = ','.join(cur.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s)", x) for x in data)
    cur.execut("""INSERT INTO esanalytics
    (ENTITY, TIMESTAMP_IST,KEYWORD,ARTICLE_URL_1, ARTICLE_TITLE_1,ARTICLE_URL_2, ARTICLE_TITLE_2,ARTICLE_URL_3, ARTICLE_TITLE_3)
    VALUES """ + args_str)
