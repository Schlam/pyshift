# pyshift
## an optimized wrapper for pushshift API

Using the undocumented multithreading method found in multithreading.pool, this wrapper can parallelize your http requests across as many threads your machine can handle. The interface is the same as ```multiprocessing```'s ```Pool``` method, but splits the job across threads which share global resources unlike ```Pool``` which creates seperate processes in their own memory space.

### How it works:
```
from multiprocessing.pool import ThreadPool
import requests


def get_docs(url):
    """
    Retrieve data using get request, left unparallelized as to go easy on the server
    """
    try:
        docs = requests.get(url).json()['data']
        return docs

    except ValueError:
        return "ValueError"
    
def get_attributes(doc):
    """
    Index document for desired data features
    """
    features=("created_utc","selftext", "title", "score")
    
    with ThreadPool(thread) as p:
        results = p.map(lambda x: doc[x],features)
        return results
    
def get_data(docs):
    """
    Map each document to the attribute function
    """
    with ThreadPool(threads) as p:
        results = p.map(get_attributes, docs)
        return results


```
