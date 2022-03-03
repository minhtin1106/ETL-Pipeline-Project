import pandas as pd
import psycopg2
from psycopg2 import Error
import numpy as np

class PostgreSQL:
    """
    This class is used to handle everything relating to PostgreSQL database ranging from 
    creating table, inserting data, to reading data
    """
    
    def __init__(self, user, password, host, db_name, port):
        """
        Constructor creates the PostgreSQL object and connects to PostgreSQL database
        It requires parameters relating to the information of the database we would like 
        to connect to
        """
        
        self.user=user
        self.password=password
        self.host=host
        self.port=port
        self.db_name=db_name
        try:
            # Connect to the database of interest 
            self.conn= psycopg2.connect(user=self.user,
                                   password=self.password,
                                   host=self.host,
                                   port=self.port,
                                   database=self.db_name
                                   )
            
            # Call a cursor to work with database
            self.cursor= self.conn.cursor()
            
            print('PostgreSQL has been successfully connected')
        except (Exception, Error) as error:
            print('Failed to connect to PostgreSQL database ', error)
        
    def create_table_in_db(self, table_name,dict_col_types):
        """
        Function is used to create a new table in database
        Function requires 2 parameters:
        + table_name which is the name of the table we want to create
        + dict_col_types which is a dictionary whose keys are column names and values are
        the  data types they store
        """
        
        try:
            # Convert the column names and data types to separate lists
            list_keys= [i for i in dict_col_types.keys()]
            list_values= [i for i in dict_col_types.values()]
            
            # Prepare syntax that is understandable for PostgreSQL 
                
                # Drop table if the table having the same name exists before
            drop_query= 'DROP TABLE IF EXISTS '+table_name
                
                # Create table
            create_query= 'CREATE TABLE '+ table_name+'('
            for idx, key in enumerate(list_keys):
                threshold= len(list_keys)-1
                if idx != threshold:
                    create_query=create_query+key +'    '+ list_values[idx]+',\n'
                else:    
                    create_query=create_query+key +'    '+ list_values[idx]+');'
            
            # Execute the queries and save 
            self.cursor.execute(drop_query) 
            self.cursor.execute(create_query)
            self.conn.commit()
            
            print('Table created successfully in PostgreSQL')
        except(Exception, Error) as error:
            print('Error while connecting to PostgreSQL', error)
        
        
    def load_data_to_db(self,table_name, list_of_col_names, obs):
        """
        Function is used to load the data to database
        Function requires 3 parameters:
        + table_name is the name of the table that we would like to load the data to
        + list_of_col_names is the list of the name of columns of that table
        + obs accepts either dataframe of observations or the array of observations 
        """
        try:
            # Prepare syntax that is understandable to PostgreSQL database
            partial_query='INSERT INTO  '+table_name+'('
            
            threshold= len(list_of_col_names)-1
            for idx, j in enumerate(list_of_col_names):
                if idx!= threshold:
                    partial_query=partial_query+' '+j+','
                else:
                    full_query=partial_query+' '+j+') VALUES(' 
           
            for idx in np.arange(len(list_of_col_names)):
                if idx!= threshold:
                    full_query+='%s,'
                else:
                    full_query+='%s)'
            
            # Identify and convert each observation to list object 
            if isinstance(obs,pd.DataFrame):
                list_load= obs.values
            else:
                list_load= obs
            
            # Execute the queries and save
            self.cursor.executemany(full_query, list_load) 
            self.conn.commit()
            
            print('Rows inserted successfully')
        except(Exception, Error) as error:
            print('Error while connecting to PostgreSQL', error)
    
    def read_data_from_db(self,table_name, col_name='all'):
        """
            Function is used to read the data from database
            Function requires two parameters:
            + table_name is the name of the table that we would like to extract data
            + col_name is a list of column names of that table
            Function returns data from the table and columns of interest
        """
        try:
            if col_name=='all':
                read_query= '''SELECT * FROM '''+ table_name
            else:
                read_query= 'SELECT '
                for idx, i in enumerate(col_name):
                    threshold=len(col_name)-1 
                    if idx!= threshold:
                        read_query+=i+','
                    else:
                        read_query+=i+'    FROM '+table_name
            self.cursor.execute(read_query)
            rows= self.cursor.fetchall()
            for row in rows:
                print(row)
            print('Read successfully')
        except(Exception, Error) as error:
            print('Error while fetching data from the PostgreSQL database',error)
            
    
    def close_connection(self):
        """
        Function is used to close the connection to the database 
        """
        if self.conn:
            self.cursor.close()
            self.conn.close()
            print('PostgreSQL connection is closed')
            

