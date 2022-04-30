import pandas as pd
import requests
import json
from operator import itemgetter

#make a call to the API and parse to json file
link = "https://api.open-meteo.com/v1/forecast?latitude=50.57&longitude=-2.45&hourly=windgusts_10m&windspeed_unit=kn&timezone=Europe%2FBerlin"
r = requests.get(link)
data = r.json()

# Opening JSON file
f = open('r.json',)
   
# returns JSON object as 
# a dictionary
data = json.load(f)
   
# Iterating through the json
# list
for i in data['hourly']:
    print(i)
   
# Closing file
f.close()
