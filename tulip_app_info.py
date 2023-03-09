# Importing Libraries
import pandas as pd
import numpy as np
import requests
from requests.auth import HTTPBasicAuth
from zipfile import ZipFile
import json
from pandas.io.json import json_normalize
import psycopg2
from datetime import datetime
from tqdm import tqdm
import warnings
warnings.filterwarnings("ignore")
import os
from configparser import ConfigParser

config = ConfigParser()
config.read('config.ini')
header_table_name = config.get('main', 'header_table_name')
detail_table_name = config.get('main', 'detail_table_name')
url_env = config.get('main', 'url_env')

def db_connection():
    connection = psycopg2.connect(
    host=config.get('main', 'host'),
    database=config.get('main', 'database'),
    user=config.get('main', 'db_user'),
    password=config.get('main', 'db_password'))
    cursor = connection.cursor()
    return connection, cursor


def api_connection(url_env):
    url = url_env
    username = config.get('main', 'api_username')
    password = config.get('main', 'api_password')
    response = requests.get(url, auth = HTTPBasicAuth(username, password))
    res_data = response.json()
    return res_data


def api_dataframe():
    res_data = api_connection(url_env)
    api_data = pd.json_normalize(res_data)
    api_data['contents'] = api_data['contents'].fillna(0)
    
    persistant_id_list = []
    group_name = []
    for i in range(len(api_data['contents'])):
        temp = []
        if (api_data['contents'][i] !=0):
            for j in range(len(api_data['contents'][i])):
                temp.append(api_data['contents'][i][j]['id'])
        else:
            temp.append('0')
        persistant_id_list.append(temp)        
    api_data['persistent_id'] = persistant_id_list
    api_data = api_data.explode('persistent_id')
    api_data = api_data.rename(columns={'id':'group_id', 'name':'group_name'}).reset_index()
    api_data = api_data[['group_id', 'group_name', 'persistent_id']]
    return api_data


def process_tulip_app(file_path):
    
    # Read the zip file and fetch the JSON
    with ZipFile(file_path) as z:      
        with z.open('app.json') as json_data:
            data = json.load(json_data)

    # Parse the JSON
    # Data Extarction - App Related Data
    df_app = pd.json_normalize(data)
    customer_origin = df_app['customer_origin'][0]
        
    # Data Extarction - Process Related Data
    df_process = pd.json_normalize(data, record_path=["process_version_sets"])

    # Data Preprocessing - Process Related Data
    df_process['created_at'] = pd.to_datetime(df_process['created_at'], unit='ms')
    df_process = df_process.rename(columns={'tags.dev.process':'process_id', 'name':'name', 
                                                              'persistent_id':'persistent_id', '_id':'app_id'})
    df_process = df_process[['app_id', 'persistent_id', 'process_id', 'name', 'created_at']]
        
    # Get api_data
    api_data = api_dataframe()
        
    # Vlookup
    tulip_info_header = df_process.merge(api_data, left_on='app_id', right_on='persistent_id', how='left')
    tulip_info_header.rename(columns={'persistent_id_x':'persistent_id'}, inplace = True)
        
    # Column app_path
    tulip_info_header['app_path'] = "https://" + customer_origin + ".tulip.co/w/1/process/"
    tulip_info_header['app_path'] = tulip_info_header.app_path.str.cat(tulip_info_header.process_id)
    tulip_info_header['app_path'] = tulip_info_header['app_path'] + "/steps/"
        
    # Final tulip_info_header Dataframe
    tulip_info_header = tulip_info_header[['app_id', 'persistent_id', 'process_id', 'name', 'group_id', 'group_name', 
                                               'app_path', 'created_at']]        

    # Data Extarction - Connector > Function Related Data
    df_connectors = pd.json_normalize(data, record_path=['connectors', 'functions'])

    # Data Preprocessing - Connector > Function Related Data
    connectors_df = df_connectors[['_id', 'name', 'query']]
    connectors_df['c_f_persistent_id'] = tulip_info_header['persistent_id'][0]
    connectors_df['c_f_process_id'] = tulip_info_header['process_id'][0]
    connectors_df = connectors_df.rename(columns={'_id':'c_f_function_id', 'name':'c_f_function_name',
                                                             'query':'c_f_function_query'})
    connectors_df = connectors_df[['c_f_persistent_id', 'c_f_process_id', 'c_f_function_id', 
                                                 'c_f_function_name', 'c_f_function_query']]

    # Data Extarction - Trigger Related Data    
    df_triggers = pd.json_normalize(data, record_path=['triggers'])

    # Data Preprocessing - Trigger Related Data
    new_col_1 = []
    new_col_2 = []
    new_col_3 = []
    temp = []
    for i in range(len(df_triggers['clauses'])):
        new_col_1.append(df_triggers['clauses'][i][0]['actions'])
    df_triggers['new_col_1'] = new_col_1

    for i in range(len(df_triggers['new_col_1'])):
        if df_triggers['new_col_1'][i] != temp:
            new_col_2.append(df_triggers['new_col_1'][i][0]['input_values'])
        else:
            new_col_2.append(temp)
    df_triggers['new_col_2'] = new_col_2

    for i in range(len(df_triggers['new_col_2'])):
        if (df_triggers['new_col_2'][i] != temp):
            if df_triggers['new_col_2'][i][0].get('functionId'):
                new_col_3.append(df_triggers['new_col_2'][i][0].get('functionId'))
            else:
                new_col_3.append(None)
        else:
            new_col_3.append(None)
    df_triggers['functionId'] = new_col_3

    triggers_df = df_triggers[['_id', 'description', 'parent_process', 'parent_step', 'parent_widget', 'functionId']]
    triggers_df = triggers_df.rename(columns={'_id':'trigger_id', 'description':'trigger_name', 
                                                  'parent_widget':'t_widget_id', 'parent_process':'t_process_id', 
                                                  'functionId':'t_function_id'})

    # Data Extarction - Step Related Data  
    df_steps = pd.json_normalize(data, record_path=['steps'])

    # Data Preprocessing - Step Related Data
    steps_df = df_steps[['_id', 'name']]
    steps_df = steps_df.rename(columns={'_id':'st_parent_step', 'name':'st_step_name'})

    # Data Extarction - Widgets Related Data  
    df_widgets = pd.json_normalize(data, record_path=['widgets'])

    # Data Preprocessing - Widgets Related Data
    widgets_df = df_widgets[['_id', 'type']]
    widgets_df = widgets_df.rename(columns={'_id':'wd_widget_id', 'type':'wd_widget_type'})

    # Joins of all data 
    m_1 = connectors_df.merge(triggers_df, left_on='c_f_function_id', right_on='t_function_id', how='left')
    m_2 = m_1.merge(steps_df, left_on='parent_step', right_on='st_parent_step', how='left')
    m_3 = m_2.merge(widgets_df, left_on='t_widget_id', right_on='wd_widget_id', how='left')

 
    tulip_info_details = m_3[['c_f_persistent_id', 'c_f_process_id', 'c_f_function_id', 'c_f_function_name', 
                              'c_f_function_query', 'trigger_id', 'trigger_name', 'parent_step', 'st_step_name', 't_widget_id', 
                              'wd_widget_type']]
        
    # Direct_link column 
    tulip_info_details = tulip_info_details.replace(np.nan, '')
    
    group_id = tulip_info_header['group_id'][0]
    app_id = tulip_info_header['app_id'][0]
    process_id = tulip_info_header['process_id'][0]
    tulip_info_details['temp_link'] = "https://" + str(customer_origin) + ".tulip.co/w/1/groups/" + \
                    str(group_id) + "/processes/" + str(app_id) +  "/versions/" + str(process_id) + "/steps/"
    tulip_info_details['direct_link'] = tulip_info_details.temp_link.str.cat(tulip_info_details.parent_step) 
        
                         
    # Final Data for tulip_info_details
    tulip_info_details = tulip_info_details[['c_f_persistent_id', 'c_f_process_id', 'c_f_function_id', 'c_f_function_name', 
                              'c_f_function_query', 'trigger_id', 'trigger_name', 'parent_step', 'st_step_name', 't_widget_id', 
                              'wd_widget_type', 'direct_link']]
    tulip_info_details = tulip_info_details.rename(columns={'c_f_persistent_id':'persistent_id', 
                                                            'c_f_process_id':'process_id', 
                                                            'c_f_function_id':'function_id',
                                                            'c_f_function_name':'function_name',
                                                            'c_f_function_query':'function_query', 
                                                            't_widget_id':'widget_id',
                                                            'st_step_name' : 'step_name',
                                                            'wd_widget_type': 'widget_type'})
       
    # DB Connection
    connection, cursor = db_connection()

    # Insert into tulip_info_header
    sql_tulipinfoheader = ("INSERT INTO {header_table} ({sql_cols}) VALUES \
                                ({placeholders}) ON CONFLICT (persistent_id, process_id) DO NOTHING"
                                .format(header_table = header_table_name, sql_cols = ", ".join([i for i in tulip_info_header.columns]), 
                            placeholders = ", ".join(["%s" for i in tulip_info_header.columns])))

    cursor.executemany(sql_tulipinfoheader, tulip_info_header.values.tolist())   
    connection.commit()

    # Delete any details for a wipe and replace
    sql_tulipinfodetails = ("delete from {detail_table} where persistent_id = %s \
                                    and process_id = %s".format(detail_table = detail_table_name))

    cursor.execute(sql_tulipinfodetails,(tulip_info_header['persistent_id'].values[0],
                                                tulip_info_header['process_id'].values[0]))   
    connection.commit()

    # Insert into tulip_info_details
    sql_tulipinfodetails = ("INSERT INTO {detail_table} (persistent_id, process_id, \
                                function_id, function_name, function_query, trigger_id, trigger_name, \
                                parent_step, step_name, widget_id, widget_type, direct_link, created_at) \
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now()) \
                                ;".format(detail_table = detail_table_name) )

    cursor.executemany(sql_tulipinfodetails, tulip_info_details.values.tolist())   
    connection.commit()

    print(f"Data Updated Successfully - {file_path}")
    print('*'*100)
   

folder_path = os.getcwd();
for filename in os.listdir(folder_path):
    if filename.endswith('.zip'):
        file_path = os.path.join(folder_path, filename)
        process_tulip_app(file_path)