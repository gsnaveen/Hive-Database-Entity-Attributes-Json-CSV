#https://mkleehammer.github.io/pyodbc/
#https://hortonworks.com/downloads/ download 64bit Hive ODBC driver
import pyodbc
import pandas as pd

hivecon = pyodbc.connect("DSN=hiveProd2;HOST=ServerName;PORT=20000;UID=userid;PWD=password!",autocommit=True)

hive_sql_db = "SHOW DATABASES like '*'"
hive_sql_tabs = "SHOW TABLES IN "
hive_sql_tabs_like = " LIKE '*'"
hive_sql_table_describe = "DESCRIBE "

tableDefall = pd.DataFrame()
TableIndexList = pd.DataFrame()

databasedf = pd.read_sql_query(hive_sql_db, hivecon)
databaselist = databasedf['database_name']
for db in databaselist:
    try:
        tablelistdf = pd.read_sql_query(hive_sql_tabs + db + hive_sql_tabs_like, hivecon)
        tablelistdfSeries = tablelistdf['tab_name']
    except:
        continue
        
    for tab in tablelistdfSeries:
        table_name = db + "." + tab
        try:
            tabledf = pd.read_sql_query(hive_sql_table_describe + table_name, hivecon)
        except:
            continue

        tabledf['database'] = db
        tabledf['table_name'] = tab
        tabledf = tabledf[(tabledf.col_name != '') &(tabledf.col_name.str.contains('#')==False ) ].drop_duplicates()
        tableDefall = tableDefall.append(tabledf,ignore_index=True)

tableDefall.to_json("HiveSchema.raw",orient='records')
rawJson = open("HiveSchema.raw", 'rU').read()
rawJson = '{ "data" : '+ rawJson +"}"
target = open("HiveSchema.json", 'w')
target.write(rawJson)
target.close()
tableDefall.to_csv("HiveSchema.csv",sep=',',header=True, index=False)
