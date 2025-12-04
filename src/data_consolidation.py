import json
from datetime import datetime, date

import duckdb
import pandas as pd

today_date = datetime.now().strftime("%Y-%m-%d")

def create_consolidate_tables():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    with open("data/sql_statements/create_consolidate_tables.sql") as fd:
        statements = fd.read()
        for statement in statements.split(";"):
            print(statement)
            con.execute(statement)

def consolidate_city_data_paris():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    raw_data_df = pd.json_normalize(data)
    raw_data_df["nb_inhabitants"] = None

    city_data_df = raw_data_df[[
        "code_insee_commune",
        "nom_arrondissement_communes",
        "nb_inhabitants"
    ]].copy()

    city_data_df.rename(columns={
        "code_insee_commune": "id",
        "nom_arrondissement_communes": "name"
    }, inplace=True)
    city_data_df.drop_duplicates(inplace=True)

    city_data_df["created_date"] = date.today()
    print(city_data_df)

    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_CITY SELECT * FROM city_data_df;")


def consolidate_city_data_nantes():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    city_data_df = pd.DataFrame([{
        "id": "44109",        
        "name": "Nantes",
        "nb_inhabitants": None, 
        "created_date": date.today()
    }])

    print(city_data_df)
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_CITY SELECT * FROM city_data_df;")

def consolidate_station_data_paris():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    raw_data_df = pd.json_normalize(data)
    raw_data_df["ADDRESS"] = None
    raw_data_df["CREATED_DATE"] = pd.Timestamp.today().normalize()
    raw_data_df["ID"] = raw_data_df["stationcode"]

    station_data_df = raw_data_df[[
        "ID",
        "stationcode",
        "name",
        "nom_arrondissement_communes",
        "code_insee_commune",
        "ADDRESS",
        "coordonnees_geo.lon",
        "coordonnees_geo.lat",
        "is_renting",
        "CREATED_DATE",
        "capacity"
    ]].copy()

    station_data_df.rename(columns={
        "stationcode": "CODE",
        "name": "NAME",
        "nom_arrondissement_communes": "CITY_NAME",
        "code_insee_commune": "CITY_CODE",
        "coordonnees_geo.lat": "LATITUDE",
        "coordonnees_geo.lon": "LONGITUDE",
        "is_renting": "STATUS",
        "capacity": "CAPACITY"
    }, inplace=True)

    station_data_df.drop_duplicates(inplace=True)

    print(station_data_df)
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM station_data_df;")

def consolidate_station_data_nantes():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    with open(f"data/raw_data/{today_date}/nantes_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    raw_data_df = pd.json_normalize(data)
    
    # Harmonisation avec le schéma Paris
    raw_data_df["ID"] = raw_data_df["number"].astype(str)
    raw_data_df["stationcode"] = raw_data_df["number"].astype(str) 
    raw_data_df["nom_arrondissement_communes"] = "Nantes"
    raw_data_df["code_insee_commune"] = "44109"  
    raw_data_df["coordonnees_geo.lon"] = raw_data_df["position.lon"]
    raw_data_df["coordonnees_geo.lat"] = raw_data_df["position.lat"]
    raw_data_df["is_renting"] = raw_data_df["status"] 
    raw_data_df["CREATED_DATE"] = pd.Timestamp.today().normalize()
    raw_data_df["ADDRESS"] = raw_data_df["address"]
    raw_data_df["capacity"] = raw_data_df["bike_stands"]

    station_data_df = raw_data_df[[
        "ID",
        "stationcode",
        "name",
        "nom_arrondissement_communes",
        "code_insee_commune",
        "ADDRESS",
        "coordonnees_geo.lon",
        "coordonnees_geo.lat",
        "is_renting",
        "CREATED_DATE",
        "capacity"
    ]].copy()

    station_data_df.rename(columns={
        "stationcode": "CODE",
        "name": "NAME",
        "nom_arrondissement_communes": "CITY_NAME",
        "code_insee_commune": "CITY_CODE",
        "coordonnees_geo.lat": "LATITUDE",
        "coordonnees_geo.lon": "LONGITUDE",
        "is_renting": "STATUS",
        "capacity": "CAPACITY"
    }, inplace=True)

    station_data_df.drop_duplicates(inplace=True)

    print(station_data_df)
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM station_data_df;")
    
def consolidate_station_statement_data_paris():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)

    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    raw_data_df = pd.json_normalize(data)
    raw_data_df["CREATED_DATE"] = pd.Timestamp.today().normalize()

    station_statement_data_df = raw_data_df[[
        "stationcode",
        "numdocksavailable",
        "numbikesavailable",
        "duedate",
        "CREATED_DATE"
    ]].copy()
    
    station_statement_data_df.rename(columns={
        "stationcode": "CODE", 
        "numdocksavailable": "BICYCLE_DOCKS_AVAILABLE",
        "numbikesavailable": "BICYCLE_AVAILABLE",
        "duedate": "LAST_STATEMENT_DATE"
    }, inplace=True)
    
    station_statement_data_df.drop_duplicates(inplace=True)

    print(station_statement_data_df)
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM station_statement_data_df;")

def consolidate_station_statement_data_nantes():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    with open(f"data/raw_data/{today_date}/nantes_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    raw_data_df = pd.json_normalize(data)
    raw_data_df["CREATED_DATE"] = pd.Timestamp.today().normalize()
    raw_data_df["stationcode"] = raw_data_df["number"].astype(str)
    raw_data_df["numdocksavailable"] = raw_data_df["available_bike_stands"]
    raw_data_df["numbikesavailable"] = raw_data_df["available_bikes"]
    raw_data_df["duedate"] = raw_data_df["last_update"]

    station_statement_data_df = raw_data_df[[
        "stationcode",
        "numdocksavailable",
        "numbikesavailable",
        "duedate",
        "CREATED_DATE"
    ]].copy()

    station_statement_data_df.rename(columns={
        "stationcode": "CODE",
        "numdocksavailable": "BICYCLE_DOCKS_AVAILABLE",
        "numbikesavailable": "BICYCLE_AVAILABLE",
        "duedate": "LAST_STATEMENT_DATE"
    }, inplace=True)

    station_statement_data_df.drop_duplicates(inplace=True)

    print(station_statement_data_df)
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM station_statement_data_df;")

