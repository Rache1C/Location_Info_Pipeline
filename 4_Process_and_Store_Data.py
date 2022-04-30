from DataSources import Extract
from DataLoad import MongoDB
import urllib
import pandas as pd
import numpy as np

class Transformation:
    
    def __init__(self, dataSource, dataSet):
      
        # creating Extract class object here, to fetch data using its generic methods for APIS and CSV data sources
        extractObj = Extract()
        
        if dataSource == 'api':
            self.data = extractObj.getAPISData(dataSet)
            funcName = dataSource+dataSet
            
            # getattr function takes in function name of class and calls it.
            getattr(self, funcName)()
        else:
            print('Unkown Data Source!!! Please try again...')
            
    # Ocean Depth Data Transformation
    def apiPollution(self):
        air_data = self.data['results']
        
        # Converting nested data into linear structure
        depth_list = []
        for data in air_data:
            for measurement in data['measurements']:
                air_dict = {}
                air_dict['city'] = data['city']
                air_dict['country'] = data['country']
                air_dict['parameter'] = measurement['parameter']
                air_dict['value'] = measurement['value']
                air_dict['unit'] = measurement['unit']
                air_list.append(air_dict)
                
        # Convert list of dict into pandas df
        df = pd.DataFrame(air_list, columns=air_dict.keys())
        
        # connection to mongo db
        mongoDB_obj = MongoDB(urllib.parse.quote_plus('root'), urllib.parse.quote_plus('password'), 'host', 'Pollution_Data')
        # Insert Data into MongoDB
        mongoDB_obj.insert_into_db(df, 'Air_Quality_India')
        
    # Pollution Data Transformation
    def apiPollution(self):
        air_data = self.data['results']
        
        # Converting nested data into linear structure
        air_list = []
        for data in air_data:
            for measurement in data['measurements']:
                air_dict = {}
                air_dict['city'] = data['city']
                air_dict['country'] = data['country']
                air_dict['parameter'] = measurement['parameter']
                air_dict['value'] = measurement['value']
                air_dict['unit'] = measurement['unit']
                air_list.append(air_dict)
                
        # Convert list of dict into pandas df
        df = pd.DataFrame(air_list, columns=air_dict.keys())
        
        # connection to mongo db
        mongoDB_obj = MongoDB(urllib.parse.quote_plus('root'), urllib.parse.quote_plus('password'), 'host', 'Pollution_Data')
        # Insert Data into MongoDB
        mongoDB_obj.insert_into_db(df, 'Air_Quality_India')

    # Crypto Market Data Transformation
    def csvCryptoMarkets(self):
        assetsCode = ['BTC','ETH','XRP','LTC']
        
        # coverting open, close, high and low price of crypto currencies into GBP values since current price is in Dollars
        # if currency belong to this list ['BTC','ETH','XRP','LTC']
        self.csv_df['open'] = self.csv_df[['open', 'asset']].apply(lambda x: (float(x[0]) * 0.75) if x[1] in assetsCode else np.nan, axis=1)
        self.csv_df['close'] = self.csv_df[['close', 'asset']].apply(lambda x: (float(x[0]) * 0.75) if x[1] in assetsCode else np.nan, axis=1)
        self.csv_df['high'] = self.csv_df[['high', 'asset']].apply(lambda x: (float(x[0]) * 0.75) if x[1] in assetsCode else np.nan, axis=1)
        self.csv_df['low'] = self.csv_df[['low', 'asset']].apply(lambda x: (float(x[0]) * 0.75) if x[1] in assetsCode else np.nan, axis=1)
        
        # dropping rows with null values by asset column
        self.csv_df.dropna(inplace=True)
        
        # saving new csv file
        self.csv_df.to_csv('crypto-market-GBP.csv')
