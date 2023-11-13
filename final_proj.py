import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime
import requests
from bs4 import BeautifulSoup


def extract(url, table_attribs):
    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    table = data.find_all('tbody')
    #print(tables)
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            if col[1].find('a') is not None:
                a_tag = col[1].find_all('a')[1]
                data_dict = {"Bank_Name": a_tag.contents[0],
                              "Market Cap": float(col[2].contents[0])}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df1], ignore_index=True)
    return df



def transform(df, csv_path):
    dataframe = pd.read_csv(csv_path) 
    dict = dataframe.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x*dict['GBP'],2) for x in df['Market Cap']]
    df['MC_EUR_Billion'] = [np.round(x*dict['EUR'],2) for x in df['Market Cap']]
    df['MC_INR_Billion'] = [np.round(x*dict['INR'],2) for x in df['Market Cap']]
    return df

def load_to_csv(df, output_csv):
  
    df.to_csv(output_csv)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe to as a database table
    with the provided name. Function returns nothing.'''

    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
   
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)
    
def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the 
    code execution to a log file. Function returns nothing.'''

    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')   

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Bank_Name', 'Market Cap']
output_csv='./Final_output.csv'
table_name='Largest_banks'
sql_connection = sqlite3.connect('Banks.db')
log_progress('Preliminaries complete. Initiating ETL process')
df = extract(url, table_attribs)
log_progress('Data extraction complete. Initiating Transformation process')
csv_path='./exchange_rate.csv'
df = transform(df,csv_path)
log_progress('Data transformation complete. Initiating loading process')
load_to_csv(df, output_csv)
log_progress('Data saved to CSV file')
sql_connection = sqlite3.connect('banks.db')
log_progress('SQL Connection initiated.')
load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as table. Running the query')
query_statement = f"SELECT * from {table_name} "
run_query(query_statement, sql_connection)
query_statement = f"SELECT AVG(MC_GBP_Billion)from {table_name} "
run_query(query_statement, sql_connection)
query_statement = f"SELECT Bank_Name ,MC_EUR_Billion from {table_name} LIMIT 5"
run_query(query_statement, sql_connection)
log_progress('Process Complete.')
sql_connection.close()
log_progress('Server Connection closed.')

