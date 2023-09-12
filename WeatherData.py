import sqlite3

class Hub:
    def __init__(self):
        self.conn = sqlite3.connect('weatherData.db')
        self.cursor = self.conn.cursor()
        self.__createTable()

    def __stationExists(self, id):
        self.cursor.execute('''SELECT * from stationData WHERE station_id = ?''', (id,))
        return self.cursor.fetchone()
    
    def __historicalDataExists(self, id, timestamp):
        self.cursor.execute('''SELECT * from stationData WHERE station_id = ? AND recorded_time = ?''', (id, timestamp))
        return self.cursor.fetchone()

    def __createTable(self):
        #Station data
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS stationData (
                station_id INTEGER,
                station_name TEXT,
                county TEXT,
                latitude REAL,
                longitude REAL,
                elevation TEXT,
                online_date TEXT,
                air_temp REAL,
                precip REAL,
                windspeed REAL,
                wind_direction REAL,
                soil_moist REAL,
                soil_temp real,
                date_interval INTEGER,
                recorded_time TEXT
            )               
        ''')
        #Historical data
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS historicalData (
                station_id INTEGER,
                air_temp REAL,
                precip REAL,
                windspeed REAL,
                wind_direction REAL,
                soil_moist REAL,
                soil_temp real,
                recorded_time TEXT
            )               
        ''')
        self.conn.commit()
    
    def insertStationData(self, stations):
        for station in stations:
            if self.__stationExists(station['stationID']):
                self.cursor.execute('''
                UPDATE stationData SET 
                air_temp = ?,
                precip = ?,
                windspeed = ?,
                wind_direction = ?,
                soil_moist = ?,
                soil_temp = ?,
                date_interval = ?,
                recorded_time = ? 
                WHERE station_id = ?''',(
                    station['airTemp'],
                    station['precip'],
                    station['windSpeed'],
                    station['windDirection'],
                    station['soilMoisture'],
                    station['soilTemp'],
                    station['dataInterval'],
                    station['recordedTime'],
                    station['stationID']))
                self.conn.commit()
            else:
                self.cursor.execute('''
                INSERT INTO stationData(station_id, station_name, county, latitude, longitude, elevation, online_date, air_temp, precip, windspeed, wind_direction, soil_moist, soil_temp, date_interval, recorded_time) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
        self.conn.commit()
    
    def insertHistoricalData(self, station):
        if self.__historicalDataExists(station['stationID'], station['recordedTime']) is None:
            self.cursor.execute('''
                INSERT INTO historicalData (station_id, air_temp, precip, windspeed, wind_direction, soil_moist, soil_temp, recorded_time) VALUES (?,?,?,?,?,?,?,?)
                ''',(station['stationID'],
                    station['airTemp'],
                    station['precip'],
                    station['windSpeed'],
                    station['windDirection'],
                    station['soilMoisture'],
                    station['soilTemp'],
                    station['recordedTime']))
            self.conn.commit()
    
    def closeConnection(self):
        self.conn.commit()
        self.conn.close()
        print("Connection closed")