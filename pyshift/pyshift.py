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
		self.today = dt.datetime.today().date() # Today's datetime object
		self.collection_name = f'{self.today}' # Name for db collection
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
		now = self.now
		unit = self.unit
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
		db_name = self.db_name
		col_name = self.get_collection_name(self.start, self.stop)
		print(f"Writing to {db_name}.{col_name}...")
		self.client[db_name][col_name].insert_many(data)
		print(f"Wrote {len(data)} items to {db_name}.{col_name}")

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
		collection = self.get_collection_name(start, stop)
		while before != f"{stop}{unit}":
			params.update({'after': after, 'before': before})
			r = requests.get(self.URL, params)
			try:
				data = r.json()['data']
				self.load_data(data)
			except Exception as e:
				
				print(f"Failed writing to {self.db_name}.{collection}")
				print(f"{type(e)}: {e}")

			after = f"{int(after[:-1]) - int(step)}{unit}"
			before = f"{int(before[:-1]) - int(step)}{unit}"
			time.sleep(1)
