import requests
from datetime import datetime, timedelta

class WeatherStations:    
    def __timeChange(self, station):
        #Change Zulu time to central time for each station recored time
        timestampStr = station['recordedTime']
        timeStamp = datetime.fromisoformat(timestampStr)
        centralTime = timeStamp - timedelta(hours=5)
        station['recordedTime'] = centralTime.strftime('%Y-%m-%dT%H:%M:%S')
        return station
    
    def getStations(self):
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
                            'county': f"{station['county']}, TX",
                            'latitude': station['latitude'],
                            'longitude': station['longitude'],
                            'elevation': station['elevation'],
                            'onlineDate': station['onlineDate'],
                            'airTemp': stationData['airTemp'],
                            'precip': stationData.get('precip', '0'),
                            'windSpeed': stationData.get('windSpeed', '0'),
                            'windDirection': stationData.get('windDirection', '0'),
                            'soilMoisture': stationData.get('soilMoisture', '0'),
                            'soilTemp': stationData.get('soilTemperature', '0'),
                            'dataInterval': stationData['dataIntervalMinutes'],
                            'recordedTime': stationData['recordedTime'],
                        }
                        approvedStations.append(self.__timeChange(approvedStation))
            return approvedStations
                        
        else:
            print('Error with requests')
    
    def getHistoricalData(self, id, minutes):
        approvedStations = []
        DataRequest = requests.get(f'https://www.texmesonet.org/api/AllChartingFieldsById/{id}/{minutes}')
        stationID = DataRequest.json()['twdbStationId']
        stationData = DataRequest.json()['values']

        for value in stationData:
            station = {
                'stationID': stationID,
                'airTemp': value['airTemp'],
                'precip': value.get('precip', '0'),
                'windSpeed': value.get('windSpeed', '0'),
                'windDirection': value.get('windDirection', '0'),
                'soilMoisture': value.get('soilMoisture', '0'),
                'soilTemp': value.get('soilTemperature', '0'),
                'recordedTime': value['dateTime']
            }
            approvedStations.append(self.__timeChange(station))
        return approvedStations