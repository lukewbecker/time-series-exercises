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

# Telco acquire function
# Being explicit in my SQL query allows me to only pull the extra info from the columns in the other tabes that I actually need, instead of returning the repeated foreign key columns. Saves me time on the prepare step.
# This acquire function will allow the user, with proper credentials to the CodeUp database access the same original dataframe that I started with in my process.
# It will first search for a csv file containing the appropriate telco data in the same folder that the jupyter notebook is being run in, and if it doesn't find one
# the function will execute the MySQL query call and create a local copy of the telco dataset.

# Recall, the way to call this function specifically is to type the following:
#   from acquire.py import get_telco_data
# That'll import this function for use in your own notebook.

def get_zillow_data():

    '''
    This function will allow the user to retrieve all tables from the Zillow database from the Codeup DB source. 
    It will acquire the data, import it as a dataframe, and then write that dataframe to a .csv file in the local directory.
    '''

    zillow_sql_query = '''
                SELECT * 
                    FROM properties_2017
                    JOIN (select id, logerror, pid, tdate FROM predictions_2017 pred_2017
                    JOIN (SELECT parcelid AS pid, Max(transactiondate) as tdate FROM predictions_2017 GROUP BY parcelid) AS sq1
                    ON (pred_2017.parcelid = sq1.pid AND pred_2017.transactiondate = sq1.tdate)) AS sq2
                    ON (properties_2017.parcelid = sq2.pid)
                    LEFT JOIN airconditioningtype USING (airconditioningtypeid)
                    LEFT JOIN architecturalstyletype USING (architecturalstyletypeid)
                    LEFT JOIN buildingclasstype USING (buildingclasstypeid)
                    LEFT JOIN heatingorsystemtype USING (heatingorsystemtypeid)
                    LEFT JOIN propertylandusetype USING (propertylandusetypeid)
                    LEFT JOIN storytype USING (storytypeid)
                    LEFT JOIN typeconstructiontype USING (typeconstructiontypeid)
                    LEFT JOIN unique_properties USING (parcelid)
                    WHERE latitude IS NOT NULL AND longitude IS NOT NULL;
                '''
    
    
    filename = 'zillow_clustering_data.csv'
    
    if os.path.isfile(filename):
        return pd.read_csv(filename)
    else:
        zillow_df = pd.read_sql(zillow_sql_query, get_db('zillow'))
        zillow_df.to_csv(filename, index = False)
        
    return zillow_df

# Finding the number of missing columns:

def zillow_missing_values(df):
    
    total_rows = df.shape[0]
    
    # Count of missing values per column
    num_row_missing = df.isna().sum()
    
    # Pct of missing values per column
    pct_rows_missing = num_row_missing/total_rows
    
    df_missing = pd.DataFrame({'num_row_missing': num_row_missing, 'pct_rows_missing': pct_rows_missing})
    
    return df_missing


# Finding the number of missing Rows:
# Much thanks to Corey for his assistance with the problem!

def count_and_percent_missing_column(df):
    num_rows = df.loc[:].isnull().sum()
    num_cols_missing = df.loc[:, df.isna().any()].count()
    pct_cols_missing = round(df.loc[:, df.isna().any()].count() / len(df.index) * 100, 3)
    missing_cols_and_rows_df = pd.DataFrame({'num_cols_missing': num_cols_missing,
                                             'pct_cols_missing': pct_cols_missing,
                                             'num_rows': num_rows})
    missing_cols_and_rows_df = missing_cols_and_rows_df.fillna(0)
    missing_cols_and_rows_df['num_cols_missing'] = missing_cols_and_rows_df['num_cols_missing'].astype(int)
    return missing_cols_and_rows_df


# Function designed to show which rows have the most missing column values, with both number of columns missing, and as a percentage of all columns (ie, % of missing column data by row)
def nulls_by_row(df):
    num_cols_missing = df.isnull().sum(axis=1)
    pct_cols_missing = df.isnull().sum(axis=1)/df.shape[1]*100
    rows_missing = pd.DataFrame({'num_cols_missing': num_cols_missing, 'pct_cols_missing': pct_cols_missing}).reset_index().groupby(['num_cols_missing','pct_cols_missing']).count().rename(index=str, columns={'index': 'num_rows'}).reset_index()
    return rows_missing 


####################### Mall data ####################### 
def get_mallcustomer_data():
    df = pd.read_sql('SELECT * FROM customers;', get_db('mall_customers'))
    return df.set_index('customer_id')


####################### REST API Acquire #######################

# Getting the sales data:

def get_sales(base_url):
    
    response = requests.get('https://python.zach.lol/api/v1/sales')
    data = response.json()
    data.keys()
    print('max_page: %s' % data['payload']['max_page'])
    print('next_page: %s' % data['payload']['next_page'])
    
    df_sales = pd.DataFrame(data['payload']['sales'])
    
    while data['payload']['next_page'] != "None":
        response = requests.get(base_url + data['payload']['next_page'])
        data = response.json()
        
        df_sales = pd.concat([df_sales, pd.DataFrame(data['payload']['sales'])])
        
        if data['payload']['next_page'] == None:
            break
            
    df_sales = df_sales.reset_index()
    print('full_shape', df_sales.shape)
    return df_sales


# Writing to csv:

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

    '''
    This function is designed to get the items data from Zach's web service and turn that data into a pandas
    dataframe for use.
    '''
    
    base_url = 'https://python.zach.lol'

    response = requests.get('https://python.zach.lol/api/v1/sales')
    data = response.json()
    data.keys()
    print('max_page: %s' % data['payload']['max_page'])
    print('next_page: %s' % data['payload']['next_page'])
    
    df_sales = pd.DataFrame(data['payload']['sales'])
    
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
    store_list = get_stores_list()
    sales_list = get_sales_data()
    
    # renaming columns:
    items_list.rename(columns = {'item_id': 'item'}, inplace = True)
    stores_df.rename(columns = {'store_id': 'store'}, inplace = True)
    
    
    # Merging the three dataframes:
    left_merge = pd.merge(sales_list, items_list, how = 'left', on = 'item')
    all_df = pd.merge(left_merge, store_list, how = 'left', on = 'store')
    
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