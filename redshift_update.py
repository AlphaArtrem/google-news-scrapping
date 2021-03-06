import csv
from pathlib import Path
import os
import json
from static import es_entities
import psycopg2
from telegram_update import telegram_updates

cred_file = open("cred.json",)
creds = json.load(cred_file)
cred_file.close()
redshift_creds = creds["redshift"]

# Search scrapper
def redshift_update():
    con = psycopg2.connect(dbname=redshift_creds["database"], host=redshift_creds["host"],
                           port=redshift_creds["port"], user=redshift_creds["username"], password=redshift_creds["password"])
    cur = con.cursor()
    # Current woking directory
    current_path = os.getcwd()
    data = []
    traversed_keywords = []
    for query in es_entities:
        # Remove spaces from queries
        query = query.replace(' ', '+')
        existing_csv_path = Path(current_path + f"/csv/{query}TopArticles.csv")
        if existing_csv_path.exists():
            with open(current_path + f"/csv/{query}TopArticles.csv", 'r', newline='') as csvfile:
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
                            ''.join(e for e in str(
                                row[2]) if e.isalnum() or e == ' '),
                            row[3],
                            ''.join(e for e in str(
                                row[4]) if e.isalnum() or e == ' '),
                            row[5],
                            ''.join(e for e in str(
                                row[6]) if e.isalnum() or e == ' '),
                            row[7],
                            ''.join(e for e in str(
                                row[8]) if e.isalnum() or e == ' '),
                        ))
                        count = 1
                        while count < 25 and row[2] not in traversed_keywords:
                            cur.execute(f"""SELECT COUNT(*) FROM esanalytics_keyword_trends 
                            WHERE TIMESTAMP_ADDED > dateadd(hour, {-1 * count}, sysdate) 
                            AND KEYWORD = '{''.join(e for e in str(row[2]) if e.isalnum() or e == ' ')}'""")
                            if cur.fetchall()[0][0] >= (4 * count):
                                count = count + 1
                            else:
                                break
                        if count > 1:
                            telegram_updates(row[0], row[1], row[2], count - 1)
                        traversed_keywords.append(row[2])
                csvfile.close()
    args = ','.join(str(x) for x in data)
    cur.execute(f"""INSERT INTO esanalytics_keyword_trends
    (ENTITY, TIMESTAMP_IST,KEYWORD,ARTICLE_URL_1, ARTICLE_TITLE_1,ARTICLE_URL_2, ARTICLE_TITLE_2,ARTICLE_URL_3, ARTICLE_TITLE_3)
    VALUES {args}""")
    con.commit()
    cur.close()
    con.close()
