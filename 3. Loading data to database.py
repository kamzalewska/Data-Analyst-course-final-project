# imports
from psycopg2 import connect
from sqlalchemy import create_engine
import pandas as pd

# connection
username = "postgres"
passwd = "####"
hostname = "localhost"
db_name = "airlines"

con = connect(user=username, password=passwd, host=hostname, database=db_name)

cursor = con.cursor()

url = f"postgresql://{username}:{passwd}@{hostname}:5432/{db_name}"
engine = create_engine(url)

# loading data
def load_raw_data(file_name):
    df_raw = pd.read_csv(f'C://####/final_project/data/raw/{file_name}', low_memory=False)
    df = df_raw.rename(columns=str.lower)
    return df


airport_df = load_raw_data('airport_list.csv')
aircraft_df = load_raw_data('aircraft.csv')
airport_weather_df = load_raw_data('airport_weather.csv')
flight_df = load_raw_data('flight.csv')

def export_table_to_db(df, table_name):
    exp_table = df.to_sql(table_name, engine, if_exists='replace', index=True, chunksize=1000, index_label='id')
    print(f'Loading data into {table_name}')
    return exp_table


export_table_to_db(aircraft_df, 'aircraft')
export_table_to_db(airport_weather_df, 'airport_weather')
export_table_to_db(flight_df, 'flight')
export_table_to_db(airport_df, 'airport_list')
