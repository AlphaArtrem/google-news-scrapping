# Google New Scraping
A google search web scrapping script to find people also searched for keywords in google news for a given query.

## Working
1. The script takes a list of queires and runs them in google news.
2. It then stores the keywords for people also searched for sections to respective csv files for each query.
3. The keywords are only added if the do not match the last three entries.
4. The list is arranged via timestamps in decending order.

## Requirements
```
requests
bs4
```