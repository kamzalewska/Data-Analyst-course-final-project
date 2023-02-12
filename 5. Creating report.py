# imports
from psycopg2 import connect

# connection
username = "postgres"
passwd = "#####"
hostname = "localhost"
db_name = "airlines"

con = connect(user=username, password=passwd, host=hostname, database=db_name)

cursor = con.cursor()

# creating views
with open(f"C://####/final_project/sql/reporting.sql", "r", encoding='utf-8-sig') as f:
    sql_file = f.read()

sql_list = sql_file.split(';')
print(sql_list)

for i in sql_list:
    cursor.execute(i)

con.commit()
