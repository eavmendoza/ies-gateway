# -*- coding: utf-8 -*-

import pandas as pd

def get_tilt_sensors(bu_id:int, conn) -> pd.DataFrame:
    query = f"""SELECT r.id as sensor_id,r.name as sensor_name,b.id as bu_id,b.name as bu_name,
                s.name as site_name,s.code as site_code, s.id as site_id, l.id as logger_id 
                FROM position_sensors  r  
                inner join loggers l on r.logger_id = l.id
                inner join sites s on l.site_id = s.id
                inner join business_units b on s.business_unit_id = b.id
                where b.id = {bu_id}
                and r.date_deactivated is null;"""


    return pd.read_sql(query, conn)

def get_soms_sensors(bu_id:int, conn) -> pd.DataFrame:
    query = f"""SELECT r.id as sensor_id,r.name as sensor_name,b.id as bu_id,b.name as bu_name,
                s.name as site_name,s.code as site_code, s.id as site_id, l.id as logger_id 
                FROM soil_moisture_sensors  r  
                inner join loggers l on r.logger_id = l.id
                inner join sites s on l.site_id = s.id
                inner join business_units b on s.business_unit_id = b.id
                where b.id = {bu_id}
                and r.name not like "Soil\_Moisture%%"
                and r.date_deactivated is null;"""

    return pd.read_sql(query, conn)

def get_rainfall_sensors(bu_id:int, conn) -> pd.DataFrame:
    query = f"""SELECT r.id as sensor_id,r.name as sensor_name,b.id as bu_id,b.name as bu_name,
                s.name as site_name,s.code as site_code, s.id as site_id, l.id as logger_id 
                FROM rain_gauge_sensors  r  
                inner join loggers l on r.logger_id = l.id
                inner join sites s on l.site_id = s.id
                inner join business_units b on s.business_unit_id = b.id
                where b.id = {bu_id}
                and r.date_deactivated is null;"""


    return pd.read_sql(query, conn)
