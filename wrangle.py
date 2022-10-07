import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from sklearn import metrics
import env
import os

#--------------------------------------------------------------------------------------------------------------------------
#acquire
def get_connection(db, user=env.username, host=env.host, password=env.password):
    return f'mysql+pymysql://{env.username}:{env.password}@{env.host}/{db}'

def get_logs():
     # if file is available locally, read it
    if os.path.isfile('curriculum_logs.csv'):
        return pd.read_csv('curriculum_logs.csv', index_col=0)  

    else:
     # if file not available locally, acquire data from SQL database
        ''' This function retrieves curriculum log from Codeup database and turns it into a dataframe'''
        query = '''
        SELECT logs.date,  logs.time,
        logs.path as endpoint,
        logs.user_id,
        logs.ip,
        cohorts.name as cohort_name,
        cohorts.start_date,
        cohorts.end_date,
        cohorts.program_id
        FROM logs
        JOIN cohorts ON logs.cohort_id= cohorts.id;
        '''
        
        
        df= pd.read_sql(query, get_connection('curriculum_logs'))
    # Write that dataframe to disk for later. This cached file will prevent repeated large queries to the database server.
        df.to_csv('curriculum_logs.csv')
    return df

    
#--------------------------------------------------------------------------------------------------------------------------
#Prepare

#functions for checking anomalies in the dataset
def one_user_df_prep(df, user):
    df = df[df.user_id == user].copy()
    #df.date = pd.to_datetime(df.date)
    #df = df.set_index(df.date)
    pages_one_user = df['endpoint'].resample('d').count()
    return pages_one_user

def compute_pct_b(pages_one_user, span, weight, user):
    midband = pages_one_user.ewm(span=span).mean()
    stdev = pages_one_user.ewm(span=span).std()
    ub = midband + stdev*weight
    lb = midband - stdev*weight
    
    bb = pd.concat([ub, lb], axis=1)
    
    my_df = pd.concat([pages_one_user, midband, bb], axis=1)
    my_df.columns = ['pages_one_user', 'midband', 'ub', 'lb']
    
    my_df['pct_b'] = (my_df['pages_one_user'] - my_df['lb'])/(my_df['ub'] - my_df['lb'])
    my_df['user_id'] = user
    return my_df

def plot_bands(my_df, user):
    fig, ax = plt.subplots(figsize=(12,8))
    ax.plot(my_df.index, my_df.pages_one_user, label='Number of Pages, User: '+str(user))
    ax.plot(my_df.index, my_df.midband, label = 'EMA/midband')
    ax.plot(my_df.index, my_df.ub, label = 'Upper Band')
    ax.plot(my_df.index, my_df.lb, label = 'Lower Band')
    ax.legend(loc='best')
    ax.set_ylabel('Number of Pages')
    plt.show()
    
def find_anomalies(df, user, span, weight, plot=False):
    pages_one_user = one_user_df_prep(df, user)
    
    my_df = compute_pct_b(pages_one_user, span, weight, user)
    
    if plot:
        plot_bands(my_df, user)
    
    return my_df[my_df.pct_b>1]   




    