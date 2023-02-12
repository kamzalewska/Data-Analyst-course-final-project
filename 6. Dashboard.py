# imports
import pandas as pd
from psycopg2 import connect
import plotly.express as px
import dash
from dash import dcc, html, dash_table, callback, Dash, Input, Output

# connection
username = "postgres"
passwd = "#####"
hostname = "localhost"
db_name = "airlines"

con = connect(user=username, password=passwd, host=hostname, database=db_name)

cursor = con.cursor()

# creating table for TOP 10 routes
top_routes_df = pd.read_sql('SELECT * FROM reporting.top_reliability_roads;', con)

top_routes_df['reliability'] = round(top_routes_df['reliability'], 2)
top_routes_df['reliability'] = top_routes_df['reliability'].apply(lambda x: str(x) + '%')

top_routes_df = top_routes_df.rename(columns={"origin_airport_name": "Origin",
                                              "dest_airport_name": "Destination",
                                              "year": "Year",
                                              "reliability": "Reliability",
                                              "cnt": "Rank"}).drop(
    columns=['origin_airport_id', 'dest_airport_id', 'nb'])

# comparison reliability year to year 2019 vs 2020
yoy_comparision_df = pd.read_sql('SELECT * FROM reporting.year_to_year_comparision;', con)

yoy_comparision_df['reliability'] = round(yoy_comparision_df['reliability']*100, 2)
yoy_comparision_df['year'] = yoy_comparision_df['year'].astype(str)

channel_top_Level = "year"
channel_2nd_Level = "month"
yoy_comparision_fig = px.bar(yoy_comparision_df, x=channel_2nd_Level, y='reliability', color=channel_top_Level)
for num, channel_top_Level_val in enumerate(yoy_comparision_df[channel_top_Level].unique()):
    temp_df = yoy_comparision_df.query(f"{channel_top_Level} == {channel_top_Level_val !r}")
    yoy_comparision_fig.data[num].x = [
            temp_df[channel_2nd_Level].tolist(),
            temp_df[channel_top_Level].tolist()
          ]
yoy_comparision_fig.layout.xaxis.title.text = f"{channel_top_Level} / { channel_2nd_Level}"
yoy_comparision_fig.layout.title.text = f"Reliability vs years and months"

# comparison day of week to day of week 2019 vs. 2020
day_to_day_comparision_df = pd.read_sql('SELECT * FROM reporting.day_to_day_comparision;', con)

day_to_day_comparision_to_plot_df = day_to_day_comparision_df
day_to_day_comparision_to_plot_df['year'] = day_to_day_comparision_to_plot_df['year'].astype(str)

channel_top_Level = "year"
channel_2nd_Level = "day_of_week"
day_to_day_comparision_fig = px.bar(
    day_to_day_comparision_to_plot_df, x=channel_2nd_Level, y='flights_amount', color=channel_top_Level)
for num, channel_top_Level_val in enumerate(day_to_day_comparision_to_plot_df[channel_top_Level].unique()):
    temp_df = day_to_day_comparision_df.query(f"{channel_top_Level} == {channel_top_Level_val !r}")
    day_to_day_comparision_fig.data[num].x = [
            temp_df[channel_2nd_Level].tolist(),
            temp_df[channel_top_Level].tolist()
          ]
day_to_day_comparision_fig.layout.xaxis.title.text = f"{channel_top_Level} / { channel_2nd_Level}"
day_to_day_comparision_fig.layout.title.text = f"Comparison of flight amount by day week 2019 vs. 2020"

# time series
day_by_day_reliability_df = pd.read_sql('SELECT * FROM reporting.day_by_day_reliability;', con)

day_by_day_reliability_fig = px.line(day_by_day_reliability_df,
                                     x="date",
                                     y="reliability",
                                     title='Reliability day by day',
                                     color=pd.DatetimeIndex(day_by_day_reliability_df['date']).year)
day_by_day_reliability_fig.layout.xaxis.title.text = f"Date"
day_by_day_reliability_fig.layout.yaxis.title.text = f"Reliability [%]"
day_by_day_reliability_fig.layout.legend.title = "Year"

# creating layouts
top_routes_table = dash_table.DataTable(
    top_routes_df.to_dict('records'), [{"name": i, "id": i} for i in top_routes_df.columns])

top_routes_page_title = html.H3(
    "TOP 10 reliability routes in 2019 and 2020",
    style={"fontFamily": "verdana", "color": "#444"}
)

top_routes_layout = dash.Dash(__name__)
top_routes_layout.layout = html.Div([
    top_routes_page_title,
    top_routes_table
    ])

yoy_comparision_component = dcc.Graph(
    id='yoy_comparision_fig',
    figure=yoy_comparision_fig
)

day_to_day_comparision_component = dcc.Graph(
    id='day_to_day_comparision_fig',
    figure=day_to_day_comparision_fig
)

comparision_layout = Dash(__name__)
comparision_layout.layout = html.Div(
    children=[
        yoy_comparision_component
    ],
)

day_by_day_layout = Dash(__name__)
day_by_day_layout.layout = html.Div(
    children=[
        day_to_day_comparision_component
    ],
)

# aplication configuration
app = dash.Dash()
app.layout = html.Div(
    children=[
        dcc.Location(id='url', refresh=False),
        dcc.Link(html.Button('TOP report', id="page_1"), href='/'),
        html.Br(),
        dcc.Link(html.Button('Comparision', id="page_2"), href='/comparision_reporting'),
        html.Br(),
        dcc.Link(html.Button('Day by day reporting', id="page_3"), href='/day_by_day_reporting'),
        html.Div(id='page-content')
    ],
)

@callback(
    Output('page-content', 'children'),
    Input('url', 'pathname'))
def display_page(pathname):
    if pathname == "/day_by_day_reporting":
        return day_by_day_layout.layout
    elif pathname == "/comparision_reporting":
        return comparision_layout.layout
    else:
        return top_routes_layout.layout

if __name__ == '__main__':
    app.run_server(debug=False, use_reloader=False)
