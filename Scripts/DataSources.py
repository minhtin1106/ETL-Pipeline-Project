import pandas as pd
import requests
import json

class Extract:
    """
    The class incorporates several methods used to fetch data having various formats
    from multiple sources (e.g: csv, json, etc)
    """
    
    def __init__(self):
        """
        Constructor creates an Extract object and assigns values to instance 
        variables (i.e: self.data_sources, self.api, self.csv_path)
        """
        # Load and assign the config file  to use it across different class methods
        self.data_sources=json.load(open('data_config.json'))
        
        # Assign the value of key 'api' and key 'csv' to instance variables
        self.api= self.data_sources['data_sources']['api']
        self.csv_path= self.data_sources['data_sources']['csv']
        
        
    def getAPIsdata(self,dataset):
        """
        Function is used to fetch the json format data from sources using api provided
        Function requires one argument which is dataset representing the name of the 
        dataset 
        Function returns json format data
        """
        api_url= self.api[dataset]
        response= requests.get(api_url)
        return response.json()
    
    def getCSVdata(self, csv_name):
        """
        Function is used to fetch the csv format data from User/ADMIN directory of the localhost
        Function requires one argument which is the csv_name (i.e: not the name of the file 
        but rather the name set in the config file)
        Function returns the csv format data
        """
        return pd.read_csv(self.csv_path[csv_name])

