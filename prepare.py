# Function module to prepare my sales data:

# Importing libraries:

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

# Importing the os library specifically for reading the csv once I've created the file in my working directory.
import os

import warnings
warnings.filterwarnings("ignore")

# web-based requests
import requests

# Time based libraries
import datetime
from time import strftime

# Importing my acquire module:
import acquire



# Main prep function, which works from my acquire file to just prior to splitting the data. Does not include any visualizations:

def prep_store_data():
    '''
    No inputs required for this function.
    This prepare function takes the data and bridges the prepare stage steps from my acquire file to just prior to splitting the data. 
    Does not include any visualizations.
    '''
    
    #Creating the df:
    if os.path.isfile('store_data.csv'):
        df = pd.read_csv('store_data.csv')
        print('Read df from .csv')
    else:
        df = acquire.my_get_store_data_read()
        print("Acquired df from database.")
    
    # Cleaning up extra index column;
    df.drop(columns = ['index'], inplace = True)
    
    # Changing the sale_date column to datetime type. Note the shrftime formatting:
    df.sale_date = pd.to_datetime(df.sale_date, format='%a, %d %b %Y %H:%M:%S %Z')
    print('Reformatted dates correctly')
    
    # I am running the sale_amount and item_price viz in a separate function.
    
    # Set the index to be the datetime variable:
    df = df.set_index('sale_date').sort_index()
    print('Set dates as index')
    
    # Adding a 'month' and 'day of week' columns:
    df["month"] = df.index.month
    df['day'] = df.index.day_name()
        
    # renaming columns:
    df = df.rename(columns = {'sale_amount': 'quantity'})
    
    # Adding column for sales_total, which is the total order: total items * item price.
    df['sales_total'] = (df.quantity) * (df.item_price)
    
    # Plotting the histograms
    df[['quantity', 'item_price']].hist()
    plt.show()
    
    return df

# Germany ops function:

def prep_ops():
    
    # acquiring the data:
    ops_df = acquire.get_germany_power()
    
    # Converting the date column to datetime:
    ops_df.date = pd.to_datetime(ops_df.date)
    
    # Setting the date as the index:
    ops_df = ops_df.set_index('date').sort_index()

    
    # Visulizing the columns:
    for col in ops_df.columns:
        plt.figure(figsize = (4, 2))
        plt.hist(ops_df[col])
        plt.title(col)
        plt.show()
    
    # Adding month and year columns:
    ops_df['month'] = ops_df.index.day
    ops_df['month'] = ops_df.index.month
    ops_df['year'] = ops_df.index.year
    
    # filling missing values:
    ops_df = ops_df.fillna(0)
    
    return ops_df

