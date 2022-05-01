import pandas as pd
import requests

import config
from config import weather_api_key

link = "https://api.open-meteo.com/v1/forecast?latitude=50.57&longitude=-2.45&hourly=windgusts_10m&windspeed_unit=kn&timezone=Europe%2FBerlin"
r = requests.get(link)
data = r.json()

# Iterating through the json
# list
def nested_dict_pairs_iterator(dict_obj):
    ''' This function accepts a nested dictionary as argument
        and iterate over all values of nested dictionaries
    '''
    # Iterate over all key-value pairs of dict argument
    for key, value in dict_obj.items():
        # Check if value is of dict type
        if isinstance(value, dict):
            # If value is dict then iterate over all its values
            for pair in nested_dict_pairs_iterator(value):
                yield (key, *pair)
        else:
            # If value is not dict type then yield the value
            yield (key, value)


for pair in nested_dict_pairs_iterator(data):
    print(pair)

# Closing file
r.close()

def get_wind_data():
    gust_list = []
    # make a call to the API and parse to json file
    link = "https://api.open-meteo.com/v1/forecast?latitude=50.57&longitude=-2.45&hourly=windgusts_10m&windspeed_unit=kn&timezone=Europe%2FBerlin"
    r = requests.get(link)
    data = r.json()

    df = pd.DataFrame(data['hourly'])
    df.to_csv('wind_by_hour.csv')

#get_wind_data()

def get_weather_data():
    _list = []
    API_key = config.weather_api_key
    # make a call to the API and parse to json file
    link = "https://api.openweathermap.org/data/2.5/onecall?lat=50.57&lon=2.45&exclude=hourly,current,minutely,alerts&appid=" + API_key
    r = requests.get(link)
    data = r.json()
    print(data)

    df = pd.DataFrame(data['daily'])
    df = df['wind_deg']
    df.to_csv('direction_by_hour.csv')

get_weather_data()



