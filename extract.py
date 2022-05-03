import pandas as pd
import requests
import config
from config import weather_api_key

# call to API to explore data set
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
def get_gust_data(lat, lon):
    gust_list = []
    # make a call to the API and parse to json file
    link = "https://api.open-meteo.com/v1/forecast?latitude=" + lat + "&longitude=" + lon + "&hourly=windgusts_10m&windspeed_unit=kn&timezone=Europe%2FBerlin"
    r = requests.get(link)
    data = r.json()

    # create df, drop index ready for merge and send to csv
    df = pd.DataFrame(data['hourly'])
    df1 = df.drop(df.index[48:])
    df1.to_csv('gusts_by_hour.csv')


# used during testing
# get_gust_data('50.57', '-2.45')

# make call to open weather map to get wind direction
def get_direction_data(lat, lon):
    _list = []
    API_key = config.weather_api_key
    # make a call to the API and parse to json file
    link = "https://api.openweathermap.org/data/2.5/onecall?lat=" + lat + "&lon=" + lon + "&exclude=daily,current,minutely,alerts&appid=" + API_key
    r = requests.get(link)
    data = r.json()

    # create df, load only wind_deg col, send to csv
    df = pd.DataFrame(data['hourly'])
    df = df['wind_deg']
    df.to_csv('direction_by_hour.csv')

# used in testing
# get_direction_data('50.57', '-2.45')


def create_data_set():
    # assign to df
    a = pd.read_csv("gusts_by_hour.csv")
    b = pd.read_csv("direction_by_hour.csv")

    # merge dfs and drop duplicate index column
    wind_predict = pd.merge(a, b)
    wind_predict = wind_predict.drop('Unnamed: 0', axis=1)

    # send to csv
    wind_predict.to_csv("wind_predict.csv")


# used in testing
# create_data_set()

# clean column order
def tidy_up():
    df = pd.read_csv('wind_predict.csv')
    # put columns into logical order
    df = df[["time", "windgusts_10m", "wind_deg"]]

    df.to_csv("wind_predict.csv")

# tidy_up()
