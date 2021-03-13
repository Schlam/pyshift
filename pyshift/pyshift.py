import time
import requests
import datetime as dt
from pymongo import MongoClient

# **This package** is a python wrapper for the [pushshift](https://pushshift.io)
# api, a fast and free source of social media data.
# ### The possibilities are endless
# - Monitor brands, keywords, and public sentiment in real time
# - Gather large quantities of text from specific forums like *r/WallStreetBets*
# - Create Time Series datasets with granularity down to **the second**

class PyShift():
	"""
	______           ______  _      _     ___        
	(_____ \         / _____)| |    (_)   / __)   _   
	_____) ) _   _ ( (____  | |__   _  _| |__  _| |_ 
	|  ____/ | | | | \____ \ |  _ \ | |(_   __)(_   _)
	| |      | |_| | _____) )| | | || |  | |     | |_ 
	|_|       \__  |(______/ |_| |_||_|  |_|      \__)
			(____/    
	"""
	def __init__(self, **kwargs):
		self.URL = 'https://api.pushshift.io/reddit/search/submission/' # Base url for API queries
		self.URI = 'mongodb://localhost:27017/' # Mongo database connection uri
		self.client = MongoClient(self.URI) # Client for interacting with mongoDB
		self.params = {'q':'bitcoin', 'sort_type':'score', 'size':'500'} # Query parameters
		self.db_name = 'bitcoin' # Database name
		self.today = dt.datetime.today() # Today's datetime object
		self.now = dt.datetime.now() # Time script was run
		self.start = 7 # Start of time series
		self.stop = 0 # End of time series
		self.unit = 'd' # Time unit
		self.step = 1 # Time increment
		for k,v in kwargs.items():
			if k in self.__dict__:
				setattr(self, k, v)
			else:
				raise KeyError(k)

	def get_collection_name(self, start, stop):
		diff = lambda t: {"m": dt.timedelta(minutes=t),
						  "s": dt.timedelta(seconds=t),
						  "h": dt.timedelta(hours=t),
						  "d": dt.timedelta(days=t)}
		from_strf = (now - diff(start).get(unit)).strftime("%H:%M_%Y-%m-%d")
		to_strf = (now - diff(stop).get(unit)).strftime("%H:%M_%Y-%m-%d")
		collection_name = from_strf + "__TO__" + to_strf
		self.collection_name = collection_name
		return collection_name

	def load_data(self, data):
		db = self.client[self.db_name]
		collection_name = get_collection_name(self.start, self.stop)
		db[collection_name].insert_many(data)
		print(f"Wrote {len(data)} items to {self.db_name}.{collection_name}")

	def get_time_series(self, **kwargs):
		for k, v in kwargs.items():
			if k in self.__dict__:
				setattr(self, k, v)
		params = self.params
		start = self.start
		stop = self.stop
		step = self.step
		unit = self.unit
		after = f"{start}{unit}"
		before = f"{int(start) - int(step)}{unit}"
		while before != f"{stop}{unit}":
			params.update({'after': after, 'before': before})
			r = requests.get(self.URL, params)
			try:
				data = r.json()['data']
				load_data(data)
			except:
				db_name = self.db_name
				collection_name = self.collection_name
				message = "Failed writing data to {db_name}.{collection_name}"
				print(message)
			after = f"{int(after[:-1])-step}{unit}"
			before = f"{int(before[:-1])-step}{unit}"
			time.sleep(1)
