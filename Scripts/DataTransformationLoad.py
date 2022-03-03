from DataSources import Extract
from Databases import PostgreSQL
import pandas as pd
import numpy as np

class Transformation_Load:
    """
    This class handles the transfomation as well as loading the transformed data to the 
    PostgreSQL database
    """
    
    def __init__(self, dataSource, dataSet):
        """
        Constructor creates a Transformation_Load object (the data needed to be transformed 
        and the newly created connection to database are assigned to instance variables: 
        i.e: self.data, and self.psql)
        """
        # Create Extract object
        extractObj = Extract()
    
        # Fetch the data needed to be transformed
        if dataSource == 'api':
            self.data= extractObj.getAPIsdata(dataSet)
        elif dataSource == 'csv':
            self.data= extractObj.getCSVdata(dataSet)
        else:
            print('Unknown Data format! Please try again')
        
        # Create connection to the database of interest
        cred= extractObj.data_sources['data_sources']['PostgreSQL_cred']
        user= cred['user']
        password= cred['password']
        host= cred['host']
        db_name= cred['db_name']
        port= cred['port']
        self.psql = PostgreSQL( user, password, host, db_name, port)
        
        
    #Covid Deaths data generic structure
    def csv_CovidDeaths(self, table_name):
        """
        Function is used to do all transformation relating to CovidDeaths data(i.e: 
        Selecting rows that have non-null values in the continent column; Total number of 
        people tested positive for COVID by country
        Function requires 1 parameter which is table_name representing the name of the table we would like to insert data into
        """

        # Drop rows having no values in continent column
        deaths_no_nul_cont= self.data[self.data['continent'].notnull()]

        # Compute total cases by country
        full2=deaths_no_nul_cont.groupby(['continent','location'])['new_cases']\
                                .agg('sum')\
                                .reset_index()\
                                .rename({'new_cases':'total_cases'},axis=1)

        # Load the transformed data to the PostgreSQL database of interest
        dict_col_types_covid={'continent':'varchar(30)',
                              'location':'varchar(50)','total_cases':'bigint'}
        list_of_col_names_covid= [i for i in full2.columns]
        self.psql.create_table_in_db(table_name, dict_col_types_covid)
        self.psql.load_data_to_db(table_name, list_of_col_names_covid,full2)
        self.psql.close_connection()
        
        
        
    #Air Pollution data generic structure
    def api_Pollution(self,  table_name):
        """
        Function is used to do all transformation relating to pollution data(i.e: 
        Selecting only relevant columns to our interest)
        Function requires 1 parameter which is table_name representing the name of the table we would like to insert data into
        """
        # Extract only the relevant columns
        air_list=[]
        for idx, ele in  enumerate (self.data['results']):
            for mea in ele['measurements']:
                air_dict={}
                air_dict['city']=ele['city']
                air_dict['coordinates']=ele['coordinates']
                air_dict['country']=ele['country']
                air_dict['parameter']=mea['parameter']
                air_dict['value']=mea['value']
                air_dict['unit']=mea['unit']
            air_list.append(air_dict)

        coor_nested = pd.DataFrame(air_list)
        full=pd.concat((coor_nested,coor_nested['coordinates'].apply(pd.Series)),axis=1)\
               .drop('coordinates',axis=1)

        # Load the transformed data to the PostgreSQL database of interest
        dict_col_types= {'city': 'varchar(50)', 'country': 'text',
                         'parameter': 'text', 'value': 'real', 'unit':'text', 
                         'latitude':'real','longitude':'real'}
        list_of_col_names= [i for i in full.columns]
        self.psql.create_table_in_db( table_name,dict_col_types)
        self.psql.load_data_to_db( table_name,list_of_col_names,full)
        self.psql.close_connection()
        
        

