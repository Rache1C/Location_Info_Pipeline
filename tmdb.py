import pandas as pd
import requests
import config
from config import weather_api_key

link = "https://api.open-meteo.com/v1/forecast?latitude=50.57&longitude=-2.45&hourly=windgusts_10m&windspeed_unit=kn&timezone=Europe%2FBerlin"
r = requests.get(link)
data = r.json()


# Iterating through the json to check key:value pairs
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


# this was run as part of examining the data before extracting
# for pair in nested_dict_pairs_iterator(data):
# print(pair)

# Closing file
r.close()


# make a call to open_meteo API to get gust data in knots
def get_gust_data():
    gust_list = []
    # make a call to the API and parse to json file
    link = "https://api.open-meteo.com/v1/forecast?latitude=50.57&longitude=-2.45&hourly=windgusts_10m&windspeed_unit=kn&timezone=Europe%2FBerlin"
    r = requests.get(link)
    data = r.json()

    #create df, drop index ready for merge and send to csv
    df = pd.DataFrame(data['hourly'])
    df1 = df.drop(df.index[48:])
    df1.to_csv('gusts_by_hour.csv')


# used during testing
# get_wind_data()


def get_direction_data():
    _list = []
    API_key = config.weather_api_key
    # make a call to the API and parse to json file
    link = "https://api.openweathermap.org/data/2.5/onecall?lat=50.57&lon=2.45&exclude=daily,current,minutely,alerts&appid=" + API_key
    r = requests.get(link)
    data = r.json()

    #create df, load only wind_deg col, send to csv
    df = pd.DataFrame(data['hourly'])
    df = df['wind_deg']
    df.to_csv('direction_by_hour.csv')


# get_weather_data()


def create_data_set():
    # assign to df
    a = pd.read_csv("gusts_by_hour.csv")
    b = pd.read_csv("direction_by_hour.csv")
    #merge dfs and drop duplicate index column
    wind_predict = pd.merge(a, b)
    wind_predict = wind_predict.drop('Unnamed: 0', axis=1)
    #swap columns to put in logical order
    wind_predict['temp'] = wind_predict['windgusts_10m']
    wind_predict['windgusts_10m'] = wind_predict['time']
    wind_predict['time'] = wind_predict['temp']
    wind_predict.drop(columns=['temp'], inplace=True)
    #send to csv
    wind_predict.to_csv("wind_predict.csv")

# create_data_set()
