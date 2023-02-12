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


# creating data frame
flight_df_raw = read_sql_table('flight')
flight_df = flight_df_raw.loc[(flight_df_raw['year'] != 2020) & (flight_df_raw['cancelled'] != 1)]
flight_df = flight_df.rename(columns={'dep_delay_new': 'dep_delay'})


# statistics and analyses of departure delay
dep_delay_statistics_df = flight_df['dep_delay'].describe(percentiles=[0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]).round(2)

minimum = int(flight_df['dep_delay'].min())
maximum = int(flight_df['dep_delay'].max())

hist_1 = plt.hist(flight_df['dep_delay'], bins=range(minimum, maximum, 10))
plt.ylabel('Frequency')
plt.xlabel('Delay (min)')
plt.title('Frequency of delays')

flight_df_2 = flight_df.loc[flight_df['dep_delay'] > 0]
hist_2 = plt.hist(flight_df_2['dep_delay'], bins=range(minimum, int(flight_df_2['dep_delay'].max()), 10))
plt.ylabel('Frequency')
plt.xlabel('Delay (min)')
plt.title('Frequency of delays for delays > 0')

flight_df_3 = flight_df_2.loc[flight_df_2['dep_delay'] < flight_df_2['dep_delay'].quantile(0.95)]
hist_3 = plt.hist(flight_df_3['dep_delay'], bins=range(minimum, int(flight_df_3['dep_delay'].max()), 10))
plt.ylabel('Frequency')
plt.xlabel('Delay (min)')
plt.title('Frequency of delays for delays > 0 and < percentile 95%')

# analysis of delays
flight_df['is_delayed'] = False
flight_df.loc[flight_df['dep_delay'] > 15, 'is_delayed'] = True

delayed_ratio = (flight_df['is_delayed'].sum()/(flight_df.shape[0])).round(2)
delayed_ratio_sum = flight_df['is_delayed'].sum()

# delays by months
flight_delays_by_month_df = round(flight_df[['month', 'is_delayed']].groupby('month').sum()
                                  / flight_df[['month', 'is_delayed']].groupby('month').count(), 2)
flight_delays_by_month_df = flight_delays_by_month_df.reset_index()

figure_1 = plt.bar(flight_delays_by_month_df['month'], flight_delays_by_month_df['is_delayed'])
plt.ylabel('Percentage of delays')
plt.xlabel('Month')
plt.title('Delay vs month')

# delays by week day
flight_delays_by_weekday_df = round(flight_df[['day_of_week', 'is_delayed']].groupby('day_of_week').sum()
                                    / flight_df[['day_of_week', 'is_delayed']].groupby('day_of_week').count(), 2)
flight_delays_by_weekday_df = flight_delays_by_weekday_df.reset_index()

figure_2 = plt.bar(flight_delays_by_weekday_df['day_of_week'], flight_delays_by_weekday_df['is_delayed'])
plt.ylabel('Percentage of delays')
plt.xlabel('Day of week')
plt.title('Delays vs day of week')

flight_df['is_weekend'] = False
flight_df.loc[(flight_df['day_of_week'] == 6)
              | (flight_df['day_of_week'] == 7), 'is_weekend'] = True

flight_delays_by_weekend_df = round(flight_df[['is_weekend', 'is_delayed']].groupby('is_weekend').sum() /
                                    flight_df[['is_weekend', 'is_delayed']].groupby('is_weekend').count(), 2)
flight_delays_by_weekend_df = flight_delays_by_weekend_df.reset_index()

figure_3 = plt.bar(flight_delays_by_weekend_df['is_weekend'], flight_delays_by_weekend_df['is_delayed'])
plt.xticks(ticks=(0, 1))
plt.ylabel('Percentage of delays')
plt.xlabel('Weekend')
plt.title('Delays vs weekend')

# delays and flight distance
flight_distance_analysis_df = flight_df['distance'].describe(
    percentiles=[0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]
).round(2)

flight_scatter = flight_df.sample(n=10000)
figure_4 = plt.scatter(flight_scatter['distance'], flight_scatter['dep_delay'])

quant = flight_df['distance'].quantile(0.95)
flight_df = flight_df.loc[flight_df['distance'] <= quant]

minimum = int(flight_df['distance'].min())
maksimum = int(flight_df['distance'].max())
flight_df['distance_agg'] = pd.cut(flight_df['distance'], bins=range(0, maksimum, 100))

flight_delays_by_distance_agg_df = round(flight_df[['is_delayed', 'distance_agg']].groupby('distance_agg').sum() /
                                         flight_df[['is_delayed', 'distance_agg']].groupby('distance_agg').count(),
                                         2).reset_index()

figure_5 = plt.bar(flight_delays_by_distance_agg_df['distance_agg'].astype(str),
                   flight_delays_by_distance_agg_df['is_delayed'].values)

plt.xticks(flight_delays_by_distance_agg_df['distance_agg'].astype(str), rotation=45)
plt.title('Delays vs flight distance')
plt.ylabel('Delays')
plt.xlabel('Flight distance')

# saving data frame
flight_df.to_csv(f'C://####/final_project/data/processed/flight_df_01.csv', index = False)