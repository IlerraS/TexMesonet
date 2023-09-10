from WeatherStation import WeatherStationHub
from WeatherData import WeatherData

weatherDB = WeatherData()

weatherHub = WeatherStationHub()

stations = weatherHub.getStations()

weatherDB.insertStationData(stations)

for station in stations:
    historicStations = weatherHub.getHistoricalData(station['stationID'], 300)
    for Hstation in historicStations:
        weatherDB.insertHistoricalData(Hstation)
        
weatherDB.closeConnection()