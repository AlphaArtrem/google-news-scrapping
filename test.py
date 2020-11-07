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
traversed_ketwords = []
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
                    traversed_ketwords.append(row[2])
                    count = 0
                    still_trending = True
                    while still_trending and count < 25:
                        cur.execute(f"""SELECT COUNT(*) FROM esanalytics_keyword_trends 
                        WHERE TIMESTAMP_ADDED > dateadd(hour, {-1 * count}, sysdate) AND KEYWORD = {row[2]}""")
                        print(cur.fetchaall()[0][0])
            csvfile.close()
cur.close() 
con.close()
