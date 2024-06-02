# Databricks notebook source
import datetime
from tqdm import tqdm    #Visão de quanto tempo falta para terminar

# COMMAND ----------


# faremos o script para iterar os dados de cada um dos meses.

#dt_start = '2021-11-01'
#dt_stop = '2022-02-01'
dt_start = dbutils.widgets.get("dt_start")
dt_stop = dbutils.widgets.get("dt_stop")

#fs_name = 'fs_assinaturas'
#fs_name = 'fs_gameplay'
fs_name = dbutils.widgets.get("feature_store")
#database = 'silver_gc'
database = dbutils.widgets.get("database_target")

database_table = "{database}.{fs_name}"

# COMMAND ----------

def import_query(path):
    with open(path, 'r') as open_file:
        query = open_file.read()
    return query

def table_exists(database, table):
    df = spark.sql(f"show tables from {database}")
    df = df.filter(f"tableName = '{table}'")
    return df.count() > 0

def date_range(start,stop):
    dates = []
    dt_start = datetime.datetetime.strptime(start, "%Y-%m-%d")
    dt_stop = datetime.datetetime.strptime(stop, "%Y-%m-%d")
    while dt_start <= dt_stop:
        dates.append(dt_start.strftime("%Y-%m-%d"))
        dt_start+=datetime.timedelta(days = 1)
    return dates

def exec_one(query, date)
    query_exec = query.format(date = date)
    df = spark.sql(query_exec)
    return df

def exec(query, dates, database, table):
    
    if not table_exists(database, table):
        date = dates.pop(0)
        df = exec_one(query, date)
        (df.coalesce(1)
        .write
        .mode("overwrite")
        .format("delta")
        .partitionBy("dtRef")
        .saveAsTable(database_table))

    for date in tqdm(dates):
        spark.sql(f"DELETE FROM {database}.{table} WHERE dtRef = '{date}' "")
        df = exec_one(query, date)
        (df.coalesce(1)
        .write
        .mode("append")
        .format("delta")
        .saveAsTable(database_table))

# COMMAND ----------

query = import_query(f"{fs_name}.sql")
dates = date_range(dt_start, dt_stop)
exec(query = query, dates = dates, database =database , table =fs_name )
