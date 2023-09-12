from WeatherStation import WeatherStations
from WeatherData import Hub

wHub = Hub()

wStations = WeatherStations()

stations = wStations.getStations()

wHub.insertStationData(stations)

for station in stations:
    historicStations = wStations.getHistoricalData(station['stationID'], 300)
    for Hstation in historicStations:
        wHub.insertHistoricalData(Hstation)
        
wHub.closeConnection()