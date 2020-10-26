# Creating the aquire.py file

# Make a new python module, acquire.py to hold the following data aquisition functions:
# get_titanic_data
# get_iris_data

# Importing libraries:

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

# Importing the os library specifically for reading the csv once I've created the file in my working directory.
import os

# web-based requests
import requests

# Make a function named get_titanic_data that returns the titanic data from the codeup data science database as a pandas data frame. Obtain your data from the Codeup Data Science Database.

# Setting up the user credentials:

from env import host, user, password

def get_db(db, user=user, host=host, password=password):
    return f'mysql+pymysql://{user}:{password}@{host}/{db}'

####################### REST API Acquire #######################

# Writing to a csv:

def write_csv(df, csv_name):
    '''
    The first argument (df) is the dataframe you want written to a .csv file. 
    The second argument (csv_name) must be a string, including the .csv extention. eg: 'example_df.csv'
    '''
    
    df.to_csv(csv_name, index = False)
    print('Completed writing df to .csv file')


# Creating the items function:

def get_items_data():
    '''
    This function is designed to get the items data from Zach's web service and turn that data into a pandas
    dataframe for use.
    '''
    base_url = 'https://python.zach.lol'
    
    # initialize:
    
    response = requests.get('https://python.zach.lol/api/v1/items')
    data = response.json()
    df = pd.DataFrame(data['payload']['items'])
    
    if os.path.isfile('items_df.csv'):
        df = pd.read_csv('items_df.csv', index_col = 0)
    else:
        for x in range(0, data['payload']['max_page']):
            response = requests.get(base_url + data['payload']['next_page'])
            data = response.json()
            df = pd.concat([df, pd.DataFrame(data['payload']['items'])], ignore_index = True)
            if data['payload']['next_page'] == None:
                return df
        df = df.reset_index()
        
    return df

# stores function:

def get_stores_list():
    '''
    This function is designed to get the items data from Zach's web service and turn that data into a pandas
    dataframe for use.
    '''
    
    base_url = 'https://python.zach.lol'
    
    # initialize:
    
    response = requests.get('https://python.zach.lol/api/v1/stores')
    data = response.json()
    df = pd.DataFrame(data['payload']['stores'])
    
    if os.path.isfile('stores_df.csv'):
        df = pd.read_csv('stores_df.csv', index_col = 0)
    else:
        if data['payload']['next_page'] == None:
            return df
        else:
            for x in range(0, data['payload']['max_page']):
                response = requests.get(base_url + data['payload']['next_page'])
                data = response.json()
                df = pd.concat([df, pd.DataFrame(data['payload']['stores'])], ignore_index = True)
            return df
        df = df.reset_index()
    return df


# Sales function:
# Thanks to Ryvyn and Corey for help!

def get_sales_data():
    
    base_url = 'https://python.zach.lol'
    
    response = requests.get('https://python.zach.lol/api/v1/sales')
    data = response.json()
    data.keys()
    print('max_page: %s' % data['payload']['max_page'])
    print('next_page: %s' % data['payload']['next_page'])
    
    df_sales = pd.DataFrame(data['payload']['sales'])
    
    
    if os.path.isfile('sales_df.csv'):
        df = pd.read_csv('sales_df.csv', index_col = 0)
    else:
        while data['payload']['next_page'] != "None":
            response = requests.get(base_url + data['payload']['next_page'])
            data = response.json()
            print('max_page: %s' % data['payload']['max_page'])
            print('next_page: %s' % data['payload']['next_page'])


            df_sales = pd.concat([df_sales, pd.DataFrame(data['payload']['sales'])])

            if data['payload']['next_page'] == None:
                break

        df_sales = df_sales.reset_index()
    print('full_shape', df_sales.shape)
    return df_sales
    

# Combining everything in acquire together into one function:

def get_store_data():
    '''
    This function will pull all the store, item and sales data from Zach's web service pages.
    This function should be the basis of where to start the prep phase.
    '''
    
    base_url = 'https://python.zach.lol'
    
    # Calling the dataframes. I need to put in a cache = True argument somewhere so it doesn't always have to be 
    # pulling from Zach's web service. I think I can put that in here but I don't recall how that works.
    
    item_list = get_items_data()
    print(item_list.shape)
    store_list = get_stores_list()
    print(store_list.shape)
    sales_list = get_sales_data()
    print(sales_list.shape)
    
    # renaming columns:
    item_list.rename(columns = {'item_id': 'item'}, inplace = True)
    store_list.rename(columns = {'store_id': 'store'}, inplace = True)
    
    # Merging the three dataframes:
    left_merge = pd.merge(sales_list, item_list, how = 'left', on = 'item')
    all_df = pd.merge(left_merge, store_list, how = 'left', on = 'store')
    
    all_df.to_csv('store_data.csv', index = False)


    return all_df



# Getting power data function:

def get_germany_power():

    power_url = 'https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv'

    df = pd.DataFrame()
    df = pd.read_csv(power_url, ',')
    
    # now the cleaning:
    df.rename(columns = {"Date": 'date', "Consumption": "consumption", "Wind": "wind", "Solar": "solar", "Wind+Solar": "wind_solar"}, inplace = True)
    
    return df 

print('End of file.')