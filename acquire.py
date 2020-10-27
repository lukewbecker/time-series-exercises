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

def my_get_store_data_read():
    '''
    This function will pull all the store, item and sales data from Zach's web service pages.
    This function should be the basis of where to start the prep phase.
    '''
    
    base_url = 'https://python.zach.lol'
    
    # Calling the dataframes. I need to put in a cache = True argument somewhere so it doesn't always have to be 
    # pulling from Zach's web service. I think I can put that in here but I don't recall how that works.
    
    if os.path.isfile('items_df.csv'):
        item_list = pd.read_csv('items_df.csv', index_col = 0)
    else:
        item_list = get_items_data()
    print(item_list.shape)

    if os.path.isfile('stores_df.csv'):
        store_list = pd.read_csv('stores_df.csv', index_col=0)
    else:
        store_list = get_stores_list()
    print(store_list.shape)

    if os.path.isfile('sales_df.csv'):
        sales_list = pd.read_csv('stores_df.csv', index_col=0)
    else:
        sales_list = get_sales_data()
    print(sales_list.shape)
    
    # renaming columns:
    # item_list.rename(columns = {'item_id': 'item'}, inplace = True)
    store_list.rename(columns = {'store_id': 'store'}, inplace = True)
    print('renamed columns')
    
    # Merging the three dataframes:
    # left_merge = pd.merge(sales_list, item_list, left_on = 'item', right_on = 'item')
    left_merge = pd.merge(sales_list, item_list, how = 'left', on = 'item')
    all_df = pd.merge(left_merge, store_list, how = 'left', on = 'store')
    
    all_df.to_csv('store_data.csv', index = False)


    return all_df



# Function that only calls the database to get the df's:

def my_get_store_data_read():
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
    print('renamed columns')
    
    # Merging the three dataframes:
    # left_merge = pd.merge(sales_list, item_list, left_on = 'item', right_on = 'item')
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




# Solutions:
######################### Helper function used in create big_df ########################################

def get_df(name):
    """
    This function takes in the string
    'items', 'stores', or 'sales' and
    returns a df containing all pages and
    creates a .csv file for future use.
    """
    base_url = 'https://python.zach.lol'
    api_url = base_url + '/api/v1/'
    response = requests.get(api_url + name)
    data = response.json()
    
    # create list from 1st page
    my_list = data['payload'][name]
    
    # loop through the pages and add to list
    while data['payload']['next_page'] != None:
        response = requests.get(base_url + data['payload']['next_page'])
        data = response.json()
        my_list.extend(data['payload'][name])
    
    # Create DataFrame from list
    df = pd.DataFrame(my_list)
    
    # Write DataFrame to csv file for future use
    df.to_csv(name + '.csv')
    return df

######################### Params Helper function, can be used in big_df ###############################

def get_df_params(name):
    """
    This function takes in the string
    'items', 'stores', or 'sales' and
    returns a df containing all pages and
    creates a .csv file for future use.
    """
    # Create an empty list names `results`.
    results = []
    
    # Create api_url variable
    api_url = 'https://python.zach.lol/api/v1/'
    
    # Loop through the page parameters until an empty response is returned.
    for i in range(3):
        response =  requests.get(items_url, params = {"page": i+1})    
    
        # We have reached the end of the results
        if len(response.json()) == 0:   
            break
            
        else:
            # Convert my response to a dictionary and store as variable `data`
            data = response.json()
        
            # Add the list of dictionaries to my list
            results.extend(data['payload'][name])
    
    # Create DataFrame from list
    df = pd.DataFrame(results)
    
    # Write DataFrame to csv file for future use
    df.to_csv(name + '.csv')
    
    return df

########################### big_df function  ######################################

def get_store_data():
    """
    This function checks for csv files
    for items, sales, stores, and big_df 
    if there are none, it creates them.
    It returns one big_df of merged dfs.
    """
    # check for csv files or create them
    if os.path.isfile('items.csv'):
        items_df = pd.read_csv('items.csv', index_col=0)
    else:
        items_df = get_df('items')
        
    if os.path.isfile('stores.csv'):
        stores_df = pd.read_csv('stores.csv', index_col=0)
    else:
        stores_df = get_df('stores')
        
    if os.path.isfile('sales.csv'):
        sales_df = pd.read_csv('sales.csv', index_col=0)
    else:
        sales_df = get_df('sales')
        
    if os.path.isfile('big_df.csv'):
        df = pd.read_csv('big_df.csv', index_col=0)
        return df
    else:
        # merge all of the DataFrames into one
        df = pd.merge(sales_df, stores_df, left_on='store', right_on='store_id').drop(columns={'store'})
        df = pd.merge(df, items_df, left_on='item', right_on='item_id').drop(columns={'item'})

        # write merged DateTime df with all data to directory for future use
        df.to_csv('big_df.csv')
        return df


print('End of file.')