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
con=psycopg2.connect(dbname= redshift_creds["database"], host=redshift_creds["host"], 
port= redshift_creds["port"], user= redshift_creds["username"], password= redshift_creds["password"])
cur = con.cursor()


cur.execute("""CREATE TABLE esanalytics_keyword_trends(
    ID INT IDENTITY(1,1),
    TIMESTAMP_IST VARCHAR (30) NOT NULL,
    TIMESTAMP_ADDED DATETIME DEFAULT CURRENT_TIMESTAMP, 
    ENTITY VARCHAR (255) NOT NULL,
    KEYWORD VARCHAR (255) NOT NULL,
    ARTICLE_TITLE_1 VARCHAR (1000) NOT NULL,  
    ARTICLE_URL_1 VARCHAR (1000) NOT NULL, 
    ARTICLE_TITLE_2 VARCHAR (1000) NOT NULL,  
    ARTICLE_URL_2 VARCHAR (1000) NOT NULL, 
    ARTICLE_TITLE_3 VARCHAR (1000) NOT NULL,  
    ARTICLE_URL_3 VARCHAR (1000) NOT NULL, 
    PRIMARY KEY (ID)
)""")
# Current woking directory
current_path = os.getcwd()
data = []
# Field for csv files
fields = []
for query in es_entities[0:1]:
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
                        'TIMESTAMP_IST' : row[0],
                        'ENTITY' : row[1],
                        'KEYWORD': row[2],
                        'ARTICLE_URL_1' : row[3],
                        'ARTICLE_TITLE_1' : row[4],
                        'ARTICLE_URL_2': row[5],
                        'ARTICLE_TITLE_3' : row[6],
                        'ARTICLE_URL_3' : row[7],
                        'ARTICLE_TITLE_3': row[8],
                    })
            csvfile.close() 
df = pd.DataFrame(data)
conn = create_engine(f'postgresql://{redshift_creds["username"]}:{redshift_creds["password"]}@{redshift_creds["host"]}:{redshift_creds["port"]}/{redshift_creds["database"]}')
df.to_sql('esanalytics_keyword_trends', conn, index=False, if_exists='append')
cur.execute("SELECT * FROM esanalytics_keyword_trends")
print(cur.fetchall())
cur.close() 
con.close()
