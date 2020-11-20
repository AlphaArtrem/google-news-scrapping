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
con=psycopg2.connect(dbname= redshift_creds["database"], host=redshift_creds["host"], 
port= redshift_creds["port"], user= redshift_creds["username"], password= redshift_creds["password"])
cur = con.cursor()
# Current woking directory
current_path = os.getcwd()
traversed_ketwords = []
cur.execute(f"""SELECT COUNT(ID), ENTITY FROM esanalytics_keyword_trends 
            WHERE TIMESTAMP_ADDED > dateadd(day, -1, sysdate)'""")
print(cur.fetchall())
cur.close() 
con.close()
