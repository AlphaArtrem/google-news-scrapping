import json
from static import es_entities
import psycopg2
from telegram_update import telegram_top_updates

cred_file = open("cred.json",)
creds = json.load(cred_file)
cred_file.close()
redshift_creds = creds["redshift"]
con=psycopg2.connect(dbname= redshift_creds["database"], host=redshift_creds["host"], 
port= redshift_creds["port"], user= redshift_creds["username"], password= redshift_creds["password"])
cur = con.cursor()

for entity in es_entities:
    cur.execute(f"""SELECT COUNT(ID), KEYWORD FROM esanalytics_keyword_trends 
                WHERE TIMESTAMP_ADDED > dateadd(day, -1, sysdate) AND ENTITY = {entity}
                GROUP BY KEYWORD ORDER BY COUNT(ID) DESC""")
    top_three = []
    if len(cur.fetchall()) >= 3 :
        top_three = cur.fetchall()[0:3] 
    else:
        top_three = cur.fetchall()
    print(entity)
    print(top_three)
cur.close() 
con.close()
