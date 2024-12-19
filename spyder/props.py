# -*- coding: utf-8 -*-
"""
Created on Thu Jun  6 06:47:56 2024

@author: earlm
"""

import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import pymysql

config_dir = 'G:/Other Computers/My Laptop/slopemonitoring-gateway/'

def connsql(db_type="logs"):
    import yaml, sys
    from pathlib import Path as path
    # cred_file = str(path.home())+'/gateway/setup/db_cred.yaml'
    cred_file = config_dir + 'setup/db_cred.yaml'
    # creds = yaml.load(open(cred_file))['DB']
    with open(cred_file, 'r') as file:
        creds = yaml.safe_load(file)
        
    creds = creds['DB']

    conn={}
    conn["host"] = creds['Host']
    conn["user"] = creds['User']
    conn["pass"] = creds['Password']
    conn["schema"] = creds['DbName']
    conn["schema_props"] = creds['DbName_props']

    if (db_type=="props"):
        props_db_conn = create_engine("mysql+pymysql://{user}:{passwd}@{host}/{schema}".format(user=conn["user"], passwd=conn["pass"], host=conn["host"], schema=conn["schema_props"]))
        return {"props": props_db_conn}
    else:
        logs_db_conn = mysql.connector.connect(user=conn["user"], password=conn["pass"],
                                  host=conn["host"], database=conn["schema"])

        return {"logs": logs_db_conn}
    
def gkn_to_json(g_query, df_name):
    import json
    
    df = pd.read_sql(g_query,conn["props"])
    indexes = pd.MultiIndex.from_frame(df[['site_code','code_name']], names=['site_code', 'code_name'])
    s = pd.Series(df['sensor_id'].to_numpy(), index=indexes)
    g = {k: s.xs(k).to_dict() for k in list(s.index.levels[0].values)}
    
    with open(f"gkn_{df_name}.json", "w") as outfile: 
        json.dump(g, outfile)
    
conn = connsql("props")

query_start = """SELECT r.id as sensor_id,r.code as sensor_name, l.id as logger_id, s.name as site_name """
query_position = "FROM position_sensors r "
query_soil = "FROM soil_moisture_sensors r "
query_rain = "FROM rain_gauge_sensors r "
query_power = "FROM position_sensors r "
query_gkn_tilt = "FROM gkn_tilt_sensors r "
query_end = """inner join loggers l on r.logger_id = l.id
        inner join sites s on l.site_id = s.id
        inner join business_units b on s.business_unit_id = b.id
        and r.date_deactivated is null"""

        
df_props=dict()
# df_props["tilt"] = pd.read_sql(query_start+query_position+query_end, conn["props"])
# df_props["soil"] = pd.read_sql(query_start+query_soil+query_end, conn["props"])
# df_props["rain"] = pd.read_sql(query_start+query_rain+query_end, conn["props"])
# df_props["power"] = pd.read_sql(query_start+query_power+query_end, conn["props"])
# df_props["gkn_tilt"] = pd.read_sql(query_start+query_gkn_tilt+query_end_2, conn["props"])

# df_props["tilt"].to_csv(config_dir+"spyder/"+"tilt_props.csv")
# df_props["soil"].to_csv(config_dir+"spyder/"+"soil_props.csv")
# df_props["rain"].to_csv(config_dir+"spyder/"+"rain_props.csv")
# df_props["power"].to_csv(config_dir+"spyder/"+"power_props.csv")
# df_props["gkn_tilt"].to_csv(config_dir+"spyder/"+"gkn_tilt_props.csv")

query_gkn_tilt = f"""SELECT s.code as site_code, r.code as code_name, r.id as sensor_id
    FROM edcslopedb_properties.gkn_tilt_sensors r 
    {query_end[:-30]}
    where b.id = 2
    """

query_gkn_soil = f"""SELECT s.code as site_code, r.code as code_name, r.id as sensor_id
    FROM edcslopedb_properties.soil_moisture_sensors r 
    {query_end[:-30]}
    where b.id = 2
    and r.code like 'Soil_Moisture%%'
    """
    
query_gkn_rain = f"""SELECT s.code as site_code, r.code as code_name, r.id as sensor_id
    FROM edcslopedb_properties.rain_gauge_sensors r 
    {query_end[:-30]}
    where b.id = 2
    and r.code like 'Rain%%'
    """
    
query_gkn_batt = f"""SELECT s.code as site_code, r.code as code_name, r.id as sensor_id
    FROM edcslopedb_properties.power_sensors r 
    {query_end[:-30]}
    where b.id = 2
    and r.code like 'DL_Batt'
    """


gkn_to_json(query_gkn_tilt, 'tilt')
gkn_to_json(query_gkn_soil, 'soil')
gkn_to_json(query_gkn_rain, 'rain')
gkn_to_json(query_gkn_batt, 'batt')






