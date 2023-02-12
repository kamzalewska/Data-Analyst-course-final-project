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


# creating data frame | adding airports

flight_df = pd.read_csv(f'C://####/final_project/data/processed/flight_df_02.csv')

airport_list_df = read_sql_table('airport_list')
# airport_list_df.loc[airport_list_df['origin_airport_id'].duplicated() == True]
# len(airport_list_df['origin_airport_id'].unique())
# airport_list_df['origin_airport_id'].shape[0]

flight_df = pd.merge(
    left=flight_df,
    right=airport_list_df[['origin_airport_id', 'origin_city_name']],
    how='left',
    on=['origin_airport_id']
)

flight_df.info()

flight_df = pd.merge(
    left=flight_df,
    right=airport_list_df[
        ['origin_airport_id', 'origin_city_name']].rename(columns={"origin_city_name": "destination_city_name"}),
    how='left',
    left_on=['dest_airport_id'],
    right_on=['origin_airport_id']
)
flight_df.info()

# adding weather details
airport_weather_df = read_sql_table('airport_weather')
airport_weather_df = airport_weather_df[['station', 'name', 'date', 'prcp', 'snow', 'snwd', 'tmax', 'awnd']]

airport_weather_df = pd.merge(
    left=airport_weather_df,
    right=airport_list_df[['name', 'origin_airport_id']],
    how='inner'
)

airport_weather_df['date'] = pd.to_datetime(airport_weather_df['date'], format='%Y-%m-%d')
flight_df['date'] = pd.to_datetime(
    (flight_df['year'].astype(str)+"-"+flight_df['month'].astype(str)+"-"+flight_df['day_of_month'].astype(str)),
    format='%Y-%m-%d')
flight = flight_df[['date', 'origin_airport_id_x', 'dep_delay']]
airport_weather_df = airport_weather_df[['date', 'snow', 'snwd', 'tmax']]
airport_weather_df = airport_weather_df[['date', 'snow', 'snwd', 'tmax']]

# testing whether weather parameters (snow, snow cover and max temperature) affect delays
flight_df = flight_df.loc[(flight_df['snow'] > 0)
                          & (flight_df['snwd'] > 0)
                          & (flight_df['tmax'] > 0)]
# for snow
delays_by_snow_stat = flight_df['snow'].describe(percentiles=[0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]).round(2)
delays_by_snow_df = flight_df[
    ['origin_airport_id_x', 'snow', 'dep_delay']].groupby('origin_airport_id_x').mean().reset_index()

figure_1 = plt.scatter(delays_by_snow_df['snow'],
                       delays_by_snow_df['dep_delay'])
plt.title('Delays vs snow')
plt.xlabel('Snow')
plt.ylabel('Delays')

# for snow cover
delays_by_snwd_stat = flight_df['snwd'].describe(percentiles=[0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]).round(2)

delays_by_snwd_df = flight_df[
    ['origin_airport_id_x', 'snwd', 'dep_delay']].groupby('origin_airport_id_x').mean().reset_index()
figure_2 = plt.scatter(delays_by_snwd_df['snwd'],
                       delays_by_snwd_df['dep_delay'])
plt.title('Delays vs snow cover')
plt.xlabel('Snow cover')
plt.ylabel('Delays')

# for max temperature
delays_by_tmax_stat = flight_df['tmax'].describe(percentiles=[0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99])

delays_by_tmax_df = flight_df[
    ['origin_airport_id_x', 'tmax', 'dep_delay']].groupby('origin_airport_id_x').mean().reset_index()
figure_3 = plt.scatter(delays_by_tmax_df['tmax'],
                       delays_by_tmax_df['dep_delay'])
plt.title('Delays vs max temperature')
plt.xlabel('Max temperature')
plt.ylabel('Delays')
