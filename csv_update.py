import time
from datetime import datetime
from pytz import timezone
import csv
from pathlib import Path
import os
from bs4 import BeautifulSoup
from telegram_update import telegram_updates


# Current woking directory
current_path = os.getcwd()


def articles_csv_update(resp, query, new_keywords):
    # Field for csv files
    fieldsArticles = ['entity', 'timestamp', 'keyword', 'article_url_1', 'article_title_1', 'article_url_2', 'article_title_2', 'article_url_3', 'article_title_3']
    all_articles = []
    # Parse data from google as html # JheGif nDgy9d
    soup = BeautifulSoup(resp.content, "html.parser")
    for g in soup.find_all('div', class_ = 'dbsr'):
        links = g.findAll('a')
        for a in links:
            try:
                text = a.find_all('div', class_ = "yr3B8d KWQBje")[0].find_all('div', class_ = "hI5pFf")[0].find_all('div', class_ = "JheGif jBgGLd")[0].text
                all_articles.append([a['href'], text])
            except:
                return
    results = []
    current_index = 0
    for keyword in new_keywords:
        current_articles = [query, keyword[0], keyword[1]]
        if current_index + 3 > len(all_articles):
            for article in all_articles[current_index : len(all_articles)]:
                current_articles = current_articles + article
            while len(current_articles) != len(fieldsArticles):
                current_articles = current_articles + ["", ""]
            current_index = 0
        else:
            for article in all_articles[current_index : current_index + 3]:
                current_articles = current_articles + article  
            current_index = current_index + 3
        results.append(current_articles)         
    # Add new result to csv
    with open(current_path + f"/csv/{query}TopArticles.csv", 'w', newline = '') as csvfile: 
        csvwriter = csv.writer(csvfile) 
        csvwriter.writerow(fieldsArticles)
        csvwriter.writerows(results)
        csvfile.close() 

def keywords_csv_update(resp, query):
    # Field for csv files
    fields = ['timestamp', 'keywords']
     # Results for current query
    results = []
    # Data from existing csv
    existing_csv_rows = []
    # Path for current query's csv file
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
                    existing_csv_rows.append(row)
            csvfile.close()
    # Parse data from google as html
    soup = BeautifulSoup(resp.content, "html.parser")
    # Get time stamp
    now_utc = datetime.now(timezone('UTC'))
    now_asia = now_utc.astimezone(timezone('Asia/Kolkata'))
    ts = now_asia.strftime("%Y-%m-%d %H:%M:%S %Z%z")
    # Find divs with class Vouh6c as it is used for people also searched sections
    for g in soup.find_all('div', class_ = 'Vouh6c'):
        new_keyword = []
        # If new csv file add result else only add if the result was not found in last 3 records
        if len(existing_csv_rows) == 0:
            new_keyword = [ts, g.text]
        else:
            found_last_six = False
            if len(existing_csv_rows) >= 6:
                for row in existing_csv_rows[0:6]:
                    if row[1] == g.text:
                        found_last_six = True
                        break
            else:
                for row in existing_csv_rows[0:len(existing_csv_rows)]:
                    if row[1] == g.text:
                        found_last_six = True
                        break
            if not found_last_six:
                new_keyword = [ts, g.text]             
        if new_keyword != []:
            results.append(new_keyword)
            telegram_updates(query, new_keyword[0], new_keyword[1])
    # If csv file did exist add its data    
    if len(existing_csv_rows) > 0:
        results.extend(existing_csv_rows)
    # Add new result to csv
    with open(current_path + f"/csv/{query}.csv", 'w', newline = '') as csvfile: 
        csvwriter = csv.writer(csvfile) 
        csvwriter.writerow(fields)
        csvwriter.writerows(results)
        csvfile.close()
    #Update articles csv for redshift
    articles_csv_update(resp, query, results)