import pandas as pd
import numpy as np
import os
from env import host, user, password
# 1.Make a function named get_titanic_data that returns the titanic data from the codeup data science database as a pandas data frame. 
#Obtain your data from the Codeup Data Science Database.

#2. Make a function named get_iris_data that returns the data from the iris_db on the codeup data science database as a pandas data frame. 
# #The returned data frame should include the actual name of the species in addition to the species_ids. 
# #Obtain your data from the Codeup Data Science Database.

#3. Once you've got your get_titanic_data and get_iris_data functions written, now it's time to add caching to them. 
# To do this, edit the beginning of the function to check for a local filename like titanic.csv or iris.csv. 
# If they exist, use the .csv file. 
# If the file doesn't exist, then produce the SQL and pandas necessary to create a dataframe, then write the dataframe to a .csv file with the appropriate name.


# 1.Make a function named get_titanic_data that returns the titanic data from the codeup data science database as a pandas data frame. 
#Obtain your data from the Codeup Data Science Database.
###################### Acquire Titanic Data ######################

def get_connection(db, user=user, host=host, password=password):
    '''
    This function uses my info from my env file to
    create a connection url to access the Codeup db.
    '''
    return f'mysql+pymysql://{user}:{password}@{host}/{db}'


def new_titanic_data():
    '''
    This function reads the titanic data from the Codeup db into a df.
    '''
    #Create SQL query.
    sql_query = 'SELECT * FROM passengers'

    #Read in DataFrame from Codeup db
    df = pd.read_sql(sql_query, get_connection('titanic_db'))

    return df

def get_titanic_data(cached=False):
    '''
    This function reads in titanic data from Codeup database and writes data to
    a csv file if cached == False or if cached == True reads in titanic df from
    a csv file, returns df.
    '''
    if cached == False or os.path.isfile('titanic_df.csv') == False :

        #read fresh data from db into a DataFrame
        df = new_titanic_data()  

        #write DataFrame to a csv file 
        df.to_csv('titanic_df.csv')

    else: 
        #If csv file exists or cahed == True, read in data from csv file
        df = pd.read_csv('titanic_df.csv', index_col=0)    

    return df                             









###################### Acquire Iris Data ######################
def new_iris_data():
    '''
    This function reads the iris data from the Codeup db into a df,
    writes it to a csv file, and returns the df.
    '''
    sql_query = """
                SELECT species_id,
                species_name,
                sepal_length,
                sepal_width,
                petal_length,
                petal_width
                FROM measurements
                JOIN species
                USING(species_id)
                """


    # Read in DataFrame from Codeup db.
    df = pd.read_sql(sql_query, get_connection('iris_db'))
    
    return df



def get_iris_data(cached= False):
    '''
    This function reads in iris data from Codeup database and writes data to
    a csv file if cached == False or if cached == True reads in iris df from
    a csv file, returns df.
    '''
    if cached == False or os.path.isfile('iris_df.csv') == False:
        #read fresh data from db into DataFrame
        df = new_iris_data()

        #cache data
        df.to_csv('iris_df.csv')
    


    else:
        
        # If csv file exists or cached == True, read in data from csv file.
        df = pd.read_csv('iris_df.csv', index_col=0)
        
    return df


