import os, sys
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from analysis.dtdef import DateWindow, today, lag_from_today
import pandas as pd

def get_moving_rainfall_sum(s_id:int, lag:int, conn) -> pd.DataFrame:
    log_start_str = lag_from_today(lag)

    # print(lag_start_dt)
    query = f"""SELECT sum(num_ticks)*.254 as mov_sum 
                FROM edcslopedb.rain_gauge_sensor_logs 
                where rain_gauge_sensor_id = {s_id} 
                and log_datetime>'{log_start_str}'"""
    
    # if window.end.string:
    #     query += f" and log_datetime < '{window.end.string} 23:59:59' "
        
    return pd.read_sql(query, conn)

# print(get_moving_rainfall_sum(29,1).values[0][0])