import os
from pymongo import MongoClient

from iot_velib.mongodb.indexes_views import prepare_mongodb
from iot_velib.velib.load_data import get_station_information
from iot_velib.mongodb.operations import update_station_information

### Connect to Database
MONGO_URI = os.environ["MONGO_URI"]

if MONGO_URI == None:
	raise ValueError('No MongoDB Cluster provided. Will exit.')
	exit(-1)

### Setup MongoDB connection
mongo_client = MongoClient(MONGO_URI)
db = mongo_client.velib
stations_collection = db.stations

### The feed to get the data
STATION_URL = "https://velib-metropole-opendata.smoove.pro/opendata/Velib_Metropole/station_information.json"

### Ensure proper status of MongoDB
prepare_mongodb(db=db, stations_collection=stations_collection)

### Load station data from Citibikes
stations = get_station_information(url=STATION_URL)

### Device Registration - Initial load and periodic refresh of stations:
update_station_information(stations=stations, collection=stations_collection, batch_size=100)
