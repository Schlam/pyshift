# Author: Sam Bellenchia
# Last Revised: 02/07/21
import requests
import time

# params = {
#   'q':q,
#   'sort_type':sort_type,
#   'sort':sort,
#   'size':size,
#   'fields':fields
# }

class PyShift:
  """PyShift class:
  arguments:

    query_params: takes a dict of query keys and values. Default 'None.' Overrides keyword arguments.
    
    agg_func: takes a function which acts on an iterable consisting of each document returned
      from a query response. If not specified, the default action is to return the number of documents.
  
    **kwargs: keyword arguments consisting of query keys and values
  methods:

    get_data(params):
      arguments:
        params: a dict of parameters specified
  """


  def __init__(self, params=None, agg_func=None, **kwargs):
    
    if params:
    
      self.params = params
    
    else:
    
      self.params = kwargs
    

    self.aggregate_function = agg_func


  def get_data(self, params=None):
    """get_data method:
    
    arguments: 

      params: dict of query key value pairs. Default `None` and uses self.params
    
    """
    
    url = 'https://api.pushshift.io/reddit/search/'
    
    if params is None:
      
      params = self.params
      

    try:
      
      r = requests.get(url, params=params)
      
      return r.json()['data']
    

    except Exception as e:
      
      print(f"{type(e)}: {e}")
      print("Returning empty list")
      
      return []

  def agg(self, data):

    if self.aggregate_function:
      
      try:
        
        return self.aggregate_function(data)
      

      except Exception as e:
        
        print(f"{type(e)}:{e}")
        
        
    return len(data)


  def get_time_series(self, params=None, start=7, stop=0, unit='d', step=1, **kwargs):
    
    if params is None:
      params = self.params
      if params is None:
        raise ValueError("No query parameters specified")

    after = f"{start}{unit}"
    before = f"{start - step}{unit}"

    results = []

    while before != f"{stop}{unit}":
      
      params.update({'after':after, 'before':before})
      data = self.get_data(params)
      
      aggregate_data = self.agg(data)
      results.append(aggregate_data)

      after = f"{int(after[:-1])-step}{unit}"
      before = f"{int(before[:-1])-step}{unit}"
      time.sleep(1)

    return results