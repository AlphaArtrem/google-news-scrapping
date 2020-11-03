import urllib
import requests
from bs4 import BeautifulSoup
import time
from csv_update import keywords_csv_update
from static import es_entities
from redshift_update import redshift_update

# Search scrapper
def google_search():
    for query in es_entities[0:1]:
        # Remove spaces from queries
        query = query.replace(' ', '+')
        # Search url
        # tbm = nws means search news section
        # gl = us means reasult for us region
        URL = f"https://google.com/search?q={query}&tbm=nws&gl=us"
        # User agent to mask script as device
        USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:65.0) Gecko/20100101 Firefox/65.0"
        headers = {"user-agent" : USER_AGENT}
        # Sending request to google
        resp = requests.get(URL, headers=headers)
        # If the request was successfull
        if resp.status_code == 200:
            keywords_csv_update(resp, query)
        else:
            print("Google deined request with error code : " + str(resp.status_code))
        time.sleep(20)

if __name__=="__main__": 
    google_search()
    redshift_update()
    
