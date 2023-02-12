# imports
from psycopg2 import connect
import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt

# connection
username = "postgres"
passwd = "####"
hostname = '127.0.0.1'
db_name = "airlines"

con = connect(user=username, password=passwd, host=hostname, database=db_name)

url = f"postgresql://{username}:{passwd}@{hostname}:5432/{db_name}"
engine = create_engine(url)

def read_sql_table(table_name):
    df = pd.read_sql(f"SELECT * FROM {table_name}", engine)
    return df


# creating data frame | adding aircraft details

flight_df = pd.read_csv(f'C://####/final_project/data/processed/flight_df_01.csv')

aircraft_df = read_sql_table('aircraft')
aircraft_df = aircraft_df.drop(['id', 'number_of_seats'], axis=1).drop_duplicates()
aircraft_df_duplicated = aircraft_df[['tail_num', 'manufacture_year']].loc[aircraft_df['tail_num'].duplicated() == True]

aircraft_df = aircraft_df.sort_values('manufacture_year', ascending=False).drop_duplicates('tail_num').sort_index()

tmp_flight_df = pd.merge(
    left=flight_df,
    right=aircraft_df,
    how='left',
    on='tail_num'
)


# looking for duplicates
duplicates_in_temp = tmp_flight_df.loc[tmp_flight_df['id'].duplicated() == True]

flight_df = tmp_flight_df.copy()
# flight_df.info()

tmp_flight_df['manufacture_year'] = tmp_flight_df['manufacture_year']

# delays and aircraft manufacture year
delays_by_manufacture_year_df = flight_df[
    ['manufacture_year', 'is_delayed']].groupby('manufacture_year').sum().reset_index()

figure_1 = plt.scatter(delays_by_manufacture_year_df['manufacture_year'], delays_by_manufacture_year_df['is_delayed'])
plt.ylabel('Delays frequency')
plt.xlabel('Manufacture year')
plt.title('Delays frequency and aircraft manufacture year')

# limiting years to those where there were more than 10 000 flights
years = flight_df[['manufacture_year', 'is_delayed', 'arr_time']].groupby('manufacture_year').count().reset_index()
delays_by_manufacture_year_df_modified = years.loc[years['arr_time'] > 10000]

figure_2 = plt.scatter(
    delays_by_manufacture_year_df_modified['manufacture_year'],
    delays_by_manufacture_year_df_modified['is_delayed'])
plt.ylabel('Delays frequency')
plt.xlabel('Manufacture year')
plt.title('Delays frequency and aircraft manufacture year')

# manufacture year grouped for periods of 3 years
first = int(flight_df['manufacture_year'].min())
last = int(flight_df['manufacture_year'].max())
flight_df['manufacture_year_agg'] = pd.cut(flight_df['manufacture_year'].sort_values(), bins=range(first, last, 3))

flight_delays_by_manufacture_year_agg_df = flight_df[
    ['manufacture_year_agg', 'is_delayed']].groupby('manufacture_year_agg').sum().reset_index()

figure_3 = plt.scatter(flight_delays_by_manufacture_year_agg_df['manufacture_year_agg'].astype(str),
                       flight_delays_by_manufacture_year_agg_df['is_delayed'])
plt.xticks(flight_delays_by_manufacture_year_agg_df['manufacture_year_agg'].astype(str), rotation=45)
plt.ylabel('Delays frequency')
plt.xlabel('Manufacture year')
plt.title('Delays frequency vs aircraft manufacture year')

# saving data frame
flight_df['manufacture_year_agg'] = flight_df['manufacture_year_agg'].astype("object")
flight_df.to_csv(f'C://####/final_project/data/processed/flight_df_02.csv', index=False)
