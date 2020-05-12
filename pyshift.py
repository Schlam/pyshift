from multiprocessing.pool import ThreadPool
from functools import lru_cache
from datetime import datetime
import requests
import time
import csv
'''
Author: Sam Bellenchia
Last Revised: 03/18/20

~~~
This tool relies heavily on the Pushshift API and vaderSentiment analysis tools

API documentation found here: 
github.com/pushshift/api

Original code was based on a repo found here:
github.com/ckw017/pushshift-nlp

Sentiment analyser found here:
github.com/vaderSentiment

'''

# Format url for specific query
def get_url(topic,content="submission",after="60s",before="0s",sort_type="score",sort_how='desc',size=1000,features=("created_utc","selftext", "title", "score","subreddit")):
    base = 'https://api.pushshift.io/reddit/search/{}/?q={}'.format(content,topic)
    query = '&after={}&before={}'.format(after, before)
    sort = '&sort_type={}&sort={}&size={}'.format(sort_type,sort_how,size)
    fields = '%s,'*len(features) % tuple(features)
    fields = '&fields={}'.format(fields[:-1])
    url = base + query + sort + fields
    return url

# Retrieve data using get request
def get_docs(url):
    time.sleep(1)
    try:
        docs = requests.get(url).json()['data']
        return docs
    except ValueError:
        return "ValueError"
    
# Index data dictionary
def get_attributes(doc):
    features=("created_utc","selftext", "title", "score","subreddit")
    with ThreadPool(4) as p:
        results = p.map(lambda x: doc[x],features)
        return results
    
# Map each document to the attribute function
def get_data(docs):
    if docs==[]:
        return "null"
    with ThreadPool(4) as p:
        results = p.map(get_attributes, docs)
        return results

# Write data to file
def write_data(data, fname):
    if data=="null":
        return "Query returned no results. No data written to csv"
    with open(fname, "wt") as f:
        writer = csv.writer(f)
        writer.writerow(["created_utc","selftext", "title", "score","subreddit"])
        for row in data:
            writer.writerow(row)
        f.close()
        return "Wrote data to {}".format(fname)


def pipe(topic,after="60s",before="0s",content="submission",sort_type="score",sort_how='desc',size=1000,features=("created_utc","selftext", "title", "score", "subreddit")):
    url = get_url(topic,after, before)
    docs = get_docs(url)
    data = get_data(docs)
    write_data(data, str(topic)+"_.csv")
    return data


def tokenize(text):
    return re.sub("\W"," ",text).rstrip().split()
