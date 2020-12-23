# Velib IoT Reference Implementation: MongoDB Atlas

This demo uses Python to freqently read the station information and station status form Velib Metropole data. It is directly stored in MongoDB Atlas.

## Architecture Overview

![](implementation.png)

## Environment Setup

### Devices
No setup for the devices is needed. Access to the [Velib Metropole GBFS Feed](https://www.velib-metropole.fr/donnees-open-data-gbfs-du-service-velib-metropole) is available free of charge and can be accessed via their [public API endpoint](https://velib-metropole-opendata.smoove.pro/opendata/Velib_Metropole/gbfs.json).

### MongoDB Atlas Cluster (Free Tier)
Please follow the instructions in the [MongoDB Atlas Getting Started Guide](https://docs.atlas.mongodb.com/getting-started/) and spin up a free tier cluster. **Please make sure to use MongoDB version 4.2 or newer**. 

You are required to whitelist the IP address of the gateway (see below), otherwise you cannot access the data in your cluster from the gateway. More advanced options like VPC peering or AWS Private Link are also available (see the [docs](https://docs.atlas.mongodb.com/setup-cluster-security/) for more details).

### Gateway 
As the gateway, we are going to use a t2.micro instance on AWS which frequently executes python scripts to read the citibike data feed and put the data into MongoDB Atlas.

As a preparation, please execute the following commands on a fresh instance (assuming Amazon Linux 2):
```
sudo yum -y update
sudo yum -y install python3 git
sudo pip3 install pymongo dnspython requests
```

Create a `.cron_profile` file in the home directory. Please replace user and password with the values you created above. Also use the connection string from the step [Connect to Your Cluster](https://docs.atlas.mongodb.com/tutorial/connect-to-your-cluster/) of the Atlas Getting Started Guide:
```
export MONGO_URI="mongodb+srv://ATLAS_USERNAME:ATLAS_PASSWORD@XXXXX.mongodb.net/test?retryWrites=true&w=majority"
```

Add the processes to the crontab via `crontab -e` and add the following commands:
```
# We leverage crontab for a frequent execution of the import from the data feeds.
# The log files will contain the results of the latest execution for debugging purposes.

# Refresh stations every hour
0 * * * * . $HOME/.cron_profile; $HOME/station_information.sh > station_information.log

# Refresh status every 30 seconds
* * * * * . $HOME/.cron_profile; $HOME/runEvery.sh 30 "$HOME/station_status.sh" > station_status.log 2>&1
```

All necessary indexes and views will be created automatically by the `station_information.py` script. As the station information will be read on an hourly basis only, please execute the following command once during the setup process to import stations on-demand:
```
cd ~
source .cron_profile
./station_information.sh
```

### Explanation of core pieces

As JSON is a frequently used data interchange format, using it in MongoDB is easy - we only need five commands to register and frequently update the master data of the bike stations in the database:
```
data = requests.get(STATION_FEED, timeout=TIMEOUT).json()

# Iterate of stations that we received from the feed
for station in data['data']['stations']:

	# We want to leverage the _id attribute of the station collection
    station['_id'] = station.pop('stationCode')

    # Convert lat/lon to a GeoJSON Point
    station['geometry'] = {'type': 'Point', 
        'coordinates': [ station.pop('lon'), station.pop('lat') ] }

    # Replace the data if the station already exists. If not, create it (via upsert=true)
    batched_operations.append(pymongo.ReplaceOne(
         { '_id': station['_id'] },
         station,
         upsert=True))

# Write the stations in a batch to the database
stations_collection.bulk_write(batched_operations)
```

The status of stations is also easy to store leveraging the bucketing pattern for efficient storage:
```
station_status = requests.get(STATUS_FEED, timeout=TIMEOUT).json()

# Iterate over status of each station
for status in station_status['data']['stations']:

	# Update an existing or create a new bucket
    batched_operations.append(pymongo.UpdateOne(
    	
    	# The station we want to add data to that has a bucket with space left
    	# We have chosen a bucket size of 60, as it will keep about an hour of data
    	# if we get new data every minute
        { 'station_id': status.pop('station_id'), 'bucket_size': { '$lt': 60 } },
        {
          # Add the new data to an array called status
          '$push': { 'status': status },

          # Keep max/min timestamp of the data in the bucket
          '$max':  { 'max_ts': status['ts'] },
          '$min':  { 'min_ts': status['ts'] },

          # Increment the bucket size by one
          '$inc':  { 'bucket_size': 1 }
         },
         upsert=True))

status_collection.bulk_write(batched_operations)
```
## Visualization
After starting the demo (which should create all the necessary collections and views in MongoDB Atlas automatically), you can create a dashboard in MongoDB Charts. [There are good introductory documentation as well as tutorials available](https://docs.mongodb.com/charts/master/).

You can create a new dashboard with a heatmap for the high turnover stations as well as a scatter plot for the current bike availability as shown in the screenshots below.

After creating two data sources for the views in MongoDB (see below for a description of the views), you can create two charts in your dashboard and configure them as following:

* Geo Scatterplot (based on the view v_bike_availability):
![](config_avail_step1.png)
![](config_avail_step2.png)

* Geo Heatmap (based on the view v_avg_hourly_utilization):
![](config_util_step1.png)
![](config_util_step2.png)

## To Do
* Machine learning integration by predicting the availability of Velib on different stations.
* Integration with a rules engine (i.e Drools)
