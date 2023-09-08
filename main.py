import requests
import sqlite3
from datetime import datetime, timedelta

approvedStations = []

stationsDataRequest = requests.get('https://www.texmesonet.org/api/Stations')
meterologicalDataRequest = requests.get('https://www.texmesonet.org/api/CurrentData')

if stationsDataRequest.status_code == 200 and meterologicalDataRequest.status_code == 200:
    stations = stationsDataRequest.json()
    currentData = meterologicalDataRequest.json()['data']

    #getting data for each station
    for station in stations:
        for stationData in currentData:
            if station['stationId'] == stationData['stationId']:
                approvedStation = {
                    'stationID': station['stationId'],
                    'stationName': station['stationName'],
                    'county': station['county'],
                    'latitude': station['latitude'],
                    'longitude': station['longitude'],
                    'elevation': station['elevation'],
                    'onlineDate': station['onlineDate'],
                    'airTemp': stationData['airTemp'],
                    'precip': stationData.get('precip', '0'),
                    'windSpeed': stationData.get('windSpeed', ''),
                    'windDirection': stationData.get('windDirection', ''),
                    'soilMoisture': stationData.get('soilMoisture', ''),
                    'soilTemp': stationData.get('soilTemperature', ''),
                    'dataInterval': stationData['dataIntervalMinutes'],
                    'recordedTime': stationData['recordedTime'],
                }
                #Change Zulu time to central time for each station recored time
                timestampStr = approvedStation['recordedTime']
                timeStamp = datetime.fromisoformat(timestampStr)
                centralTime = timeStamp - timedelta(hours=5)
                approvedStation['recordedTime'] = centralTime.strftime('%Y-%m-%dT%H:%M:%S')
                approvedStations.append(approvedStation)
else:
    print('Error with requests')



try:
    #connect the database
    connection = sqlite3.connect('MesonetData.db')
    cursor = connection.cursor()

    #creating stations table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stations (
                station_id INTEGER,
                station_name TEXT,
                county TEXT,
                latitude REAL,
                longitude REAL,
                timestamp TEXT,
                elevation TEXT,
                online_date TEXT,
                airTemp REAL,
                precip REAL,
                windspeed REAL,
                wind_direction REAL,
                soil_Moist REAL,
                date_interval INTEGER,
                recorded_time TEXT,
                PRIMARY KEY (station_id)
        )               
    ''')
    #inserting each approved station into the table
    for station in approvedStations:
        cursor.execute('''
            INSERT INTO stations(station_id, station_name, county, latitude, longitude, timestamp, elevation, online_date, airTemp, precip, windspeed, wind_direction, soil_Moist, date_interval, recorded_time) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ''',(station['stationID'],
                    station['stationName'],
                    station['county'],
                    station['latitude'],
                    station['longitude'],
                    station['elevation'],
                    station['onlineDate'],
                    station['airTemp'],
                    station['precip'],
                    station['windSpeed'],
                    station['windDirection'],
                    station['soilMoisture'],
                    station['soilTemp'],
                    station['dataInterval'],
                    station['recordedTime'])
                    )
        connection.commit()

except sqlite3.Error as e:
    print(f"SQLite error: {e}")

finally:
    connection.close()