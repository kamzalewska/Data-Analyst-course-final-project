# imports
import requests
import time
import pandas as pd

# creating API connection
url = 'https://api-datalab.coderslab.com/api/'
headers = {
      'accept': 'application/json', 'authorization': '####'
}

# loading list of airports
df = pd.read_csv('C://####/final_project/data/airports_smaller.csv')
list_airports_id = list(df['origin_airport_id'].unique())

# downloading airports id
airports_details = []

for airport_id in list_airports_id:
    response = requests.get(
            f'{url}airport/{airport_id}',
            headers=headers)
    time.sleep(0.02)
    if response.status_code != 400:
        airports_details.append(response.json())
    else:
        continue


airport_df = pd.DataFrame.from_records(airports_details)
# list_airports_id_cleaned = list(airport_df['ORIGIN_AIRPORT_ID'].unique())

airport_df = airport_df.to_csv('C://####/final_project/data/raw/airport_list.csv', index=False)

# downloading weather
list_weather_months = pd.date_range('2019-01', '2020-04', freq='M').strftime("%Y-%m").tolist()
# len(list_weather_months)

weather_details = []

for weather_date in list_weather_months:
    time.sleep(0.02)
    response = requests.get(f'{url}airportWeather?date={weather_date}', headers=headers)
    weather_details.extend(response.json())

airport_weather_df = pd.DataFrame.from_records(weather_details)

airport_weather_df.to_csv('C://####/final_project/data/raw/airport_weather.csv', index=False)

# downloading aircraft details
aircraft_api_response = requests.get(f'{url}aircraft', headers=headers)
aircraft_response = aircraft_api_response.json()
aircraft_df = pd.DataFrame.from_records(aircraft_response)

aircraft_df.to_csv('C://####/final_project/data/raw/aircraft.csv', index=False)

# downloading flights
flight_details = []

for airport_id in list_airports_id:
    for date in list_weather_months:
        # print(f'Dowloading data from airport {airport_id} at {date}')
        response = requests.get(f'{url}flight?airportId={airport_id}&date={date}', headers=headers)
        # check_limit()
        flight_details.extend(response.json())
        time.sleep(0.2)

flight_df = pd.DataFrame.from_records(flight_details)

flight_df.to_csv('C://####/final_project/data/raw/flight.csv', index=False)
