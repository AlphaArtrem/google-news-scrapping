import csv
from pathlib import Path
import os
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
                    data.append((
                        row[0],
                        row[1],
                        row[2],
                        row[3],
                        ''.join(e for e in str(row[4]) if e.isalnum() or e == ' '),
                        row[5],
                        ''.join(e for e in str(row[6]) if e.isalnum() or e == ' '),
                        row[7],
                        ''.join(e for e in str(row[8]) if e.isalnum() or e == ' '),
                        ))
            csvfile.close() 
cur.execute("""INSERT INTO esanalytics_keyword_trends
(ENTITY, TIMESTAMP_IST,KEYWORD,ARTICLE_URL_1, ARTICLE_TITLE_1,ARTICLE_URL_2, ARTICLE_TITLE_2,ARTICLE_URL_3, ARTICLE_TITLE_3)
VALUES """ + tuple(data))
con.commit()
cur.close() 
con.close()
