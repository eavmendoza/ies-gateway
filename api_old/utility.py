# from FlaskApp import app
# import flaskapp
# import Resource, Api
# import Resource
import pandas as pd
# import mysql.connector as sql
import numpy
import math
import json
from datetime import datetime, timedelta
# import urllib.parse as urlparse
# from urllib.parse import parse_qs
# import subprocess as sub
# import configparser

DEFAULT_START_DT = 7
SHORT_WINDOW = 3
        
def get_conn(dbname="logs"):
    import yaml
    from sqlalchemy import create_engine
    import pymysql
    import os
    # os.chdir("G:/Other computers/My Laptop/server/flaskappserver/")
    # sys.path.insert(0, '/var/www/FlaskDash')
    # fname = "G:/Other\ computers/My\ Laptop/server/flaskappserver/credentials.yaml"
    fname = "credentials.yaml"
    # fname = '/var/www/FlaskDash/credentials.yaml'
    # conn = yaml.safe_load(open(fname))
    with open(fname, 'r') as f:
        conn = yaml.full_load(f)
    # conn = yaml.safe_load(open(fname))
    conn = conn['DB']

    # db_connection = sql.connect(host=conn["Host"], database=conn["DbName"], 
    #     user=conn["User"], password=conn["Password"])
    
    if (dbname=="props"):
        dbname_conn="DbName_properties"
    elif (dbname=="analysis"):
        dbname_conn="DbName_analysis"
    else:
        dbname_conn="DbName"
    
    db_connection = create_engine("mysql+pymysql://{user}:{passwd}@{host}/{schema}".format(
        user=conn["User"], 
        passwd=conn["Password"], 
        host=conn["Host"], 
        schema=conn[dbname_conn]))


    return db_connection

def get_start_dt(dt_str=None, delay=None):

    if not delay:
        delay = DEFAULT_START_DT

    try:
        datetime.strptime(dt_str, "%Y-%m-%d")
        start_dt = dt_str
    except (ValueError, TypeError):
        start_dt = datetime.now() - timedelta(days=delay)
        start_dt = start_dt.strftime('%Y-%m-%d')
    
    return start_dt

def get_volt(volt_id=None, start_dt=None, latest=False, delay=None):

    db_connection = get_conn()
    start_dt = get_start_dt()

    query = ("select log_datetime as dt, voltage "
        "from power_sensor_logs "
        "where power_sensor_id = {} "
        "and log_datetime > \"{}\"").format(volt_id, start_dt)
    df = pd.read_sql(query, con=db_connection).set_index("dt")

    if df.empty:
        error = {
            "Error type" : "Value Error",
            "Message" : "No available data",
            "Sensor type" : "Voltage",
            "Sensor id": str(volt_id)
        }
        return format_output(error)

    if latest:
        df_out = df.tail(3)
    else:
        df_out = df

    return format_output(df_out)

def get_rain(rain_id, start_dt=None, latest=False, delay=None):

    db_connection = get_conn()
    start_dt = get_start_dt(start_dt, delay)

    query = ("select log_datetime as dt, num_ticks*0.254 as rainfall "
             "from edcslopedb.rain_gauge_sensor_logs "
             "where rain_gauge_sensor_id = {} "
             "and log_datetime > \"{}\"").format(rain_id, start_dt)
    df = pd.read_sql(query, con=db_connection).set_index("dt")

    if df.empty:
        error = {
            "Error type" : "Value Error",
            "Message" : "No available data",
            "Sensor type" : "Rain gauge",
            "Sensor id": str(rain_id)
        }
        return json.loads(json.dumps(error))

    df2 = pd.DataFrame()
    #df_10M = df.resample("10T").first().fillna(0.0)

    try:
        df = df.resample("10T").first()
    except TypeError:
        df_out = None
        return json.loads(df_out.to_json(orient="table", date_unit="s"))
    
    df_10M_w_null = df.resample("10T").first() 
    
    df_10M = df_10M_w_null.fillna(0.0).round(2)
    df_60M = df_10M.rolling(window=6).sum().round(2)
    df_24H = df_10M.rolling(window=24*6).sum().round(2)
    df_3D = df_10M.rolling(window=3*24*6).sum().round(2)

    df_out = pd.concat([df_10M, df_60M, df_24H, df_3D, df_10M_w_null], axis=1)

    if latest:
        df_out = df_out.tail()

    df_out.columns = ['10M','60M','24H','3D','10M_W_NULL']

    return json.loads(df_out.to_json(orient="table", date_unit="s"))

def outlier_filter(dff):
    
    dff = dff.resample('10min').first().ffill()
    
    dfmean = dff[['ax_x','ax_y','ax_z']].rolling(min_periods=10,window='24h',center=False).mean()
    dfsd = dff[['ax_x','ax_y','ax_z']].rolling(min_periods=10,window='24h',center=False).std()
    #setting of limits
    dfulimits = dfmean + (3*dfsd)
    dfllimits = dfmean - (3*dfsd)
    
    # print(len(dff))
    
    
    dff.loc[(dff['ax_x'] > dfulimits['ax_x']) | (dff['ax_x'] < dfllimits['ax_x']), 'ax_x'] = numpy.nan
    dff.loc[(dff['ax_y'] > dfulimits['ax_y']) | (dff['ax_y'] < dfllimits['ax_y']), 'ax_y'] = numpy.nan
    dff.loc[(dff['ax_z'] > dfulimits['ax_z']) | (dff['ax_z'] < dfllimits['ax_z']), 'ax_z'] = numpy.nan
    
    # dff.ax_y[(dff.ax_y > dfulimits.ax_y) | (dff.ax_y < dfllimits.ax_y)] = numpy.nan
    # dff.ax_z[(dff.ax_z > dfulimits.ax_z) | (dff.ax_z < dfllimits.ax_z)] = numpy.nan
    
    # dflogic = dff.ax_x * dff.ax_y * dff.ax_z
    
    #dff = dff.loc[dff['ax_x'].notnull() & dff['ax_z'].notnull() & dff['ax_z'].notnull()]
    
    # print(">",len(dff))
   
    return dff

def orthogonal_filter(df):

    # remove all non orthogonal value
    # dfo = df[['ax_x','ax_y','ax_z']]/1
    mag = (df.ax_x*df.ax_x + df.ax_y*df.ax_y + df.ax_z*df.ax_z).apply(numpy.sqrt)
    lim = .05
    
    return df.loc[((mag>(1-lim)) & (mag<(1+lim)))]

def get_vel_query(tilt_ids, dt_from, dt_to, dir_request):

    # s.s_id as s_id,
    dir_query = {
        "down": "atan2(r.zn, sqrt(pow(xn,2)+pow(yn,2)))/PI()*180",
        "across": "atan2(r.yn, sqrt(pow(xn,2)+pow(zn,2)))/PI()*180"
    }
    query = """
        select 
          avg(s.dir) as dir,
          concat(sites.code,"-", substring(r.code,4,3)) as sensor_name,
          from_unixtime(floor(unix_timestamp(s.log_datetime)/(10*60))*10*60) as log_datetime
        from (
          select 
            {dir_query} as dir,
            r.s_id as s_id,
            r.log_datetime as log_datetime
          from ( 
            SELECT 
              ax_x/sqrt(pow(ax_x,2)+pow(ax_y,2)+pow(ax_y,2)) as xn,
              ax_y/sqrt(pow(ax_x,2)+pow(ax_y,2)+pow(ax_y,2)) as yn,
              ax_z/sqrt(pow(ax_x,2)+pow(ax_y,2)+pow(ax_y,2)) as zn,
              q.position_sensor_id as s_id,
              q.log_datetime
            FROM edcslopedb.position_sensor_logs q
            WHERE q.position_sensor_id in ( {tilt_ids} )
            and log_datetime between from_unixtime({dt_from}/1000) and from_unixtime({dt_to}/1000)
          ) r
        ) s
        
        inner join edcslopedb_properties.position_sensors r on s.s_id = r.id
        inner join edcslopedb_properties.loggers l on r.logger_id = l.id
        inner join edcslopedb_properties.sites sites on l.site_id = sites.id
        group by log_datetime, sensor_name
    """.format(tilt_ids=tilt_ids, dt_from=dt_from, dt_to=dt_to, dir_query=dir_query[dir_request])

    return query

def get_tilt_query(tilt_id, start_dt):

    query = "SELECT log_datetime as dt,"

    axes = "ax_x, ax_y, ax_z"
    query = "{} {}".format(query, axes)

    query = "{} FROM position_sensor_logs where position_sensor_in = ({}) ".format(query, tilt_id)
    query = "{} and log_datetime between from_unixtime({}) and from_unixtime({})".format(query, start_dt)

    return query

def get_tilt_query_bulk_computed(tilt_ids, dt_from, dt_to):
    query = """
        select
          min(s.ax_x) as ax_x,
          min(s.ax_y) as ax_y,
          min(s.ax_z) as ax_z,
          min(s.position_sensor_id) as position_sensor_id,
          log_datetime as dt
        from
          (
            SELECT
              avg(asin(d_logs.ax_x) * 180 / PI()) over (
                partition by position_sensor_id
                order by
                  log_datetime range between interval '24' hour preceding
                  and current row
              ) as ax_x,
              avg(asin(d_logs.ax_y) * 180 / PI()) over (
                partition by position_sensor_id
                order by
                  log_datetime range between interval '24' hour preceding
                  and current row
              ) as ax_y,
              avg(asin(d_logs.ax_z) * 180 / PI()) over (
                partition by position_sensor_id
                order by
                  log_datetime range between interval '24' hour preceding
                  and current row
              ) as ax_z,
              d_logs.position_sensor_id as position_sensor_id,
              d_logs.log_datetime
            FROM
              edcslopedb.position_sensor_logs d_logs
              
            WHERE
              d_logs.position_sensor_id in ({tilt_ids})
              and log_datetime between from_unixtime({dt_from}/1000) and from_unixtime({dt_to}/1000)
          ) s
        group by
          log_datetime
    """.format(tilt_ids=tilt_ids, dt_from=dt_from, dt_to=dt_to)
    
    # print(query)

    return query

def get_tilt_query_bulk(tilt_ids, dt_from, dt_to):
    query = """
        select
          log_datetime as log_datetime,
          min(s.position_sensor_id) as position_sensor_id,
          avg(s.ax_x) as ax_x,
          avg(s.ax_y) as ax_y,
          avg(s.ax_z) as ax_z
        from
          (
        	SELECT
        	  d_logs.ax_x as ax_x,
        	  d_logs.ax_y as ax_y,
        	  d_logs.ax_z as ax_z,
        	  d_logs.position_sensor_id as position_sensor_id,
        	  from_unixtime(floor(unix_timestamp(d_logs.log_datetime)/(10*60))*10*60) as log_datetime
        	FROM
        	  edcslopedb.position_sensor_logs d_logs              
        	WHERE
              d_logs.ax_x < 1 and
              d_logs.ax_y < 1 and
              d_logs.ax_z < 1 and
              d_logs.position_sensor_id in ({tilt_ids}) and
              d_logs.log_datetime between from_unixtime({dt_from}/1000) and from_unixtime({dt_to}/1000)
         ) s
         group by s.log_datetime, s.position_sensor_id 
          
    """.format(tilt_ids=tilt_ids, dt_from=dt_from, dt_to=dt_to)
    
    # print(query)

    return query

def recursive_dict_clean(d):
    for k, v in d.items():
        if isinstance(v, list):
            v[:] = [i for i in v if i]
        if isinstance(v, dict):
            recursive_dict_clean(v)

def format_output(out):
    if type(out) is dict:
        return json.loads(json.dumps(out))
    
    elif type(out) is pd.core.frame.DataFrame:
        out = json.loads(out.to_json(orient="table", date_unit="s", double_precision=4))
        return out

def normalize(df):
    df['xn'] = df.ax_x / numpy.sqrt(df.ax_x*df.ax_x + df.ax_y*df.ax_y + df.ax_z*df.ax_z)
    df['yn'] = df.ax_y / numpy.sqrt(df.ax_x*df.ax_x + df.ax_y*df.ax_y + df.ax_z*df.ax_z)
    df['zn'] = df.ax_z / numpy.sqrt(df.ax_x*df.ax_x + df.ax_y*df.ax_y + df.ax_z*df.ax_z)
    return df[['xn','yn','zn','position_sensor_id']]

def project(df):
    df['down'] = numpy.arctan2(df.zn,(numpy.sqrt(df.xn**2 + df.yn**2)))
    df['across'] = numpy.arctan2(df.yn,(numpy.sqrt(df.xn**2 + df.zn**2)))

    return df[['down','across','position_sensor_id']]

def get_tilt2(tilt_id, start_dt=None, latest=False, delay=None, maf=True, vel=False, dir_re=None):

    db_connection = get_conn()
    start_dt = get_start_dt(start_dt, delay)

    query = get_tilt_query(tilt_id, start_dt)

    df = pd.read_sql(query, con=db_connection).set_index("dt")

    # df = outlier_filter(df)
    # df = orthogonal_filter(df)

    if df.empty:
        error = {
            "Error type" : "Value Error",
            "Message" : "No available data",
            "Sensor type" : "AMH tilt sensor",
            "Sensor id": str(tilt_id)
        }
        return format_output(error)


    df_out = None
    if vel:
        WIN = int(24*60/10)
        df = df.resample('10T').mean().rolling(window=WIN,min_periods=2).mean()
        

        try:
            df_vel = project(normalize(df))
            df_vel = df_vel - df_vel.shift(periods=6)

            df_vel = numpy.arcsin(df_vel)*180/math.pi
            df_vel = df_vel[['down','across']]

            if latest:
                df_out = df_vel.tail(3)
            else:
                df_out = df_vel
        except:
            error = {
            "Error type" : "Value Error",
            "Message" : "Computation error (velocity)",
            "Sensor type" : "AMH tilt sensor",
            "Sensor id": str(tilt_id)
            }
            return format_output(error)
    else:
        df_deg = numpy.arcsin(df)*180/math.pi
        if maf:
            df_deg = df_deg.resample('10T').mean().rolling(window=WIN,
                min_periods=2).mean()

        if latest:
            df_out = df_deg.tail(3)
        else:
            df_out = df_deg

    df_out=df

    return format_output(df_out)

# def get_tilt3(tilt_ids, dt_from, dt_to, vel_dir):
    
def get_vel_bulk(df):
    df_vel = project(normalize(df))
    df_vel[['down','across']] = df_vel[['down','across']] - df_vel[['down','across']].shift(periods=6)

    df_vel[['down','across']] = numpy.arcsin(df_vel[['down','across']])*180/math.pi
    df_vel = df_vel[['down','across','position_sensor_id']]
    
    return df_vel 
    
def get_tilt5(tilt_ids, dt_from, dt_to, vel=False):

    db_connection = get_conn("analysis")
    
    query = get_tilt_query_bulk(tilt_ids, dt_from, dt_to)
    df = pd.read_sql(query, con=db_connection).set_index("log_datetime")
    
    df = df.groupby("position_sensor_id", 
                    group_keys=False)[['ax_x','ax_y','ax_z','position_sensor_id']].apply(
                        lambda x: orthogonal_filter(outlier_filter(x)))
                        
    if vel:
        df_vel = df.groupby("position_sensor_id", 
                        group_keys=False)[['ax_x','ax_y','ax_z',
                                            'position_sensor_id']].apply(
                            lambda x: get_vel_bulk(x))
                            # lambda x: normalize(x))
        return df_vel
    
    return df

    db_connection = get_conn()
    
    
    query = get_vel_query(tilt_ids, dt_from, dt_to, vel_dir)

    df = pd.read_sql(query, con=db_connection)

    # df = outlier_filter(df)
    # df = orthogonal_filter(df)

    if df.empty:
        error = {
            "Error type" : "Value Error",
            "Message" : "No available data",
            "Sensor type" : "AMH tilt sensor",
            "Sensor id": str(tilt_ids)
        }
        return format_output(error)

    # create pivot table based on sensor id and log_datetime
    df = df.pivot_table(columns="sensor_name", values="dir", index="log_datetime")

    # resample and applying a 24hr moving average filter
    win=int(24*60/10) 
    df = df.resample('10Min').mean().rolling(window=win,min_periods=2).mean()

    # get velocity by shifting
    PER = int(60/10)
    df = df-df.shift(periods=PER)
    df = df.abs()

    return format_output(df)

def get_tilt4(tilt_ids, dt_from, dt_to):

    db_connection = get_conn()
         
    query = get_tilt_query_bulk(tilt_ids, dt_from, dt_to)

    # import time
    # start = time.time()
    df = pd.read_sql(query, con=db_connection).set_index("log_datetime")
    
    # print(time.time()-start)
    
    # start = time.time()
    df = df.groupby("position_sensor_id", 
                    group_keys=False)[['ax_x','ax_y','ax_z','position_sensor_id']].apply(
                        lambda x: orthogonal_filter(outlier_filter(x)))

    if df.empty:
        error = {
            "Error type" : "Value Error",
            "Message" : "No available data",
            "Sensor type" : "AMH tilt sensor"
        }
        return format_output(error)

    return (df)


def get_tilt(tilt_id, start_dt=None, latest=False, delay=None, maf=True):

    db_connection = get_conn()
    start_dt = get_start_dt(start_dt, delay)

    if int(tilt_id) < 10 or int(tilt_id) >= 24:
        query = ("SELECT log_datetime as dt, ax_x, ax_y, ax_z "
            "FROM position_sensor_logs where position_sensor_id = {} "
            "and log_datetime > \"{}\" and ax_x > 0.0").format(tilt_id,
            start_dt)
    elif int(tilt_id) < 24 and int(tilt_id) > 14:
        query = ("SELECT log_datetime as dt, -ax_x as ax_x, -ax_y as ax_y, ax_z "
            "FROM position_sensor_logs where position_sensor_id = {} "
            "and log_datetime > \"{}\" and -ax_x > 0.1").format(tilt_id,
            start_dt)

    df = pd.read_sql(query, con=db_connection).set_index("dt")

    df = outlier_filter(df)
    df = orthogonal_filter(df)

    if df.empty:
        error = {
            "Error type" : "Value Error",
            "Message" : "No available data",
            "Sensor type" : "AMH tilt sensor",
            "Sensor id": str(tilt_id)
        }
        return json.loads(json.dumps(error))

    try:
        with numpy.errstate(invalid='raise'):
            df_deg = numpy.arcsin(df)*180/math.pi
    except (TypeError,FloatingPointError):
        df_deg = None
        return json.loads(df_deg.to_json(orient="table", date_unit="s", 
        double_precision=3))

    if maf:
        WIN = int(24*60/10)
        df_deg = df_deg.resample('10T').mean().rolling(window=WIN,min_periods=2).mean()
    
    if latest:
        df_deg = df_deg.tail(3)

    return json.loads(df_deg.to_json(orient="table", date_unit="s", 
        double_precision=3))

def get_gkn_tilt(gkn_tilt_id, start_dt=None, latest=False, delay=None):

    db_connection = get_conn()
    start_dt = get_start_dt(start_dt, delay)

    query = ("select log_datetime as dt, angle_a, angle_b from gkn_tilt_sensor_logs "
        "where gkn_tilt_sensor_id={id} "
        "and log_datetime > \"{start_dt}\"".format(id=gkn_tilt_id, start_dt=start_dt))

    df = pd.read_sql(query, con=db_connection).set_index("dt")

    if df.empty:
        error = {
            "Error type" : "Value Error",
            "Message" : "No available data",
            "Sensor type" : "Geokon tilt",
            "Sensor id": str(gkn_tilt_id)
        }
        return json.loads(json.dumps(error))

    try:
        df = df.resample("10T").first()
    except TypeError:
        df = None
        return json.loads(df.to_json(orient="table", date_unit="s", 
            double_precision=7))

    if latest:
        df = df.tail(3)    

    return json.loads(df.to_json(orient="table", date_unit="s", 
        double_precision=7))

def get_soms(soms_id, start_dt=None, latest=False, delay=None, maf=True):

    db_connection = get_conn()
    start_dt = get_start_dt(start_dt, delay)

    query = ("select log_datetime as dt, ratio as vwc "
        "from soil_moisture_sensor_logs where soil_moisture_sensor_id = {soms_id} "
        "and log_datetime > \"{dt}\" and abs(ratio)<1.0;").format(dt=start_dt,soms_id=soms_id)

    df = pd.read_sql(query, con=db_connection).set_index("dt")

    if df.empty:
        error = {
            "Error type" : "Value Error",
            "Message" : "No available data",
            "Sensor type" : "Soil moisture",
            "Sensor id": str(soms_id)
        }
        return json.loads(json.dumps(error))

    try:
        df = df.resample("10T").first()
    except TypeError:
        df = None
        return json.loads(df.to_json(orient="table", date_unit="s", double_precision=3))

    # if int(soms_id) < 49:
    #     df = df["vwc"]*100.00
    if df["vwc"].mean() < 1.0:
        df = df["vwc"]*100.00

    if maf:
        WIN = int(24*60/10)
        df = df.resample('10T').mean().rolling(window=WIN,min_periods=2).mean()

    if latest:
        df = df.tail(3)  

    return json.loads(df.to_json(orient="table", date_unit="s", 
        double_precision=3))

def get_network(logger_id, start_dt=None, latest=False, delay=None, maf=True):
    db_connection = get_conn()
    start_dt = get_start_dt(start_dt, delay)

    query = ("SELECT stat_datetime as dt, net_stat "
        "FROM edcslopedb.gateway_network_status "
        "where logger_id={n_id} "
        "and stat_datetime>'{start_dt}'").format(start_dt=start_dt,n_id=logger_id)

    df = pd.read_sql(query, con=db_connection).set_index("dt")

    if df.empty:
        error = {
            "Error type" : "Value Error",
            "Message" : "No available data",
            "Sensor type" : "Gateway Network status",
            "Loggers id": str(logger_id)
        }
        return json.loads(json.dumps(error))

    df.loc[df["net_stat"]==2, 'net_stat'] = 0

    if latest:
        df = df.tail(3)  
    
    return json.loads(df.to_json(orient="table", date_unit="s", 
        double_precision=3))

    
def insert_on_conflict_update(table, conn, keys, data_iter):
    from sqlalchemy.dialects.mysql import insert
    data = [dict(zip(keys, row)) for row in data_iter]
    stmt = (
        insert(table.table)
        .values(data)
    )
    stmt = stmt.on_duplicate_key_update(
        ax_x=stmt.inserted.ax_x, 
        ax_y=stmt.inserted.ax_y,
        ax_z=stmt.inserted.ax_z)
    result = conn.execute(stmt)
    return result.rowcount

def run_filter():
    from itertools import tee
    from pandas.tseries.offsets import Day
    d1 = pd.date_range("2022-11-10", "2024-12-12", freq="W-MON")
    dti = d1.union(d1 + Day(7))
    
    from itertools import islice
    def get_next(some_iterable, window=1):
        items, nexts = tee(some_iterable, 2)
        nexts = islice(nexts, window, None)
        return zip(items, nexts)
    # 
    # conn=get_conn("analysis")
    
    for end, start in get_next(list(dti)[::-1]):
        print(start, end, flush=True)
        start_unix = int(start.timestamp()*1000)
        end_unix = int(end.timestamp()*1000)
        print(start_unix, end_unix, flush=True)
        
        apo = "90,91,123,127,128,136,108,109,110,88,89,129,152,153,154,155,156,157"
        bacman = "75,76,69,70,71,72,73,74,80,81,101,77,78,79,82,83,84,85,86,87"
        negros = "105,106,107,102,103,104,116,117,118,119,120,121,124,125,126,133,134,135"
        amacan = "137,138,139,140,141,142,143,144,145,146,147,148,149,150,151"
        dfg = get_tilt4(
            tilt_ids=amacan, 
            dt_from=start_unix, 
            dt_to=end_unix
            )
        
        dfg.index.names=["log_datetime"]
                
        conn=get_conn("analysis")
        try:
            dfg.to_sql(con=conn, name="filtered_position_sensor_logs", 
                       if_exists="append", schema="analysisdb", 
                       method=insert_on_conflict_update)
        except AttributeError:
            print(dfg)

def run_regular_tilt_filter():
    apo = "90,91,123,127,128,136,108,109,110,88,89,129,152,153,154,155,156,157"
    bacman = "75,76,69,70,71,72,73,74,80,81,101,77,78,79,82,83,84,85,86,87"
    negros = "105,106,107,102,103,104,116,117,118,119,120,121,124,125,126,133,134,135"
    amacan = "137,138,139,140,141,142,143,144,145,146,147,148,149,150,151"
    leyte = "93,94,95,96,97,98,111,112,113,114,115"

    tilt_ids_list = [apo, bacman, negros, amacan, leyte]

    from datetime import datetime as dt
    from datetime import timedelta as td

    for ids in tilt_ids_list:
        # print(start, end, flush=True)
        start_unix = int((dt.today()-td(days=3)).timestamp()*1000)
        end_unix = int(dt.today().timestamp()*1000)
        print(start_unix, end_unix, flush=True)
        
        dfg = get_tilt4(
            tilt_ids=ids, 
            dt_from=start_unix, 
            dt_to=end_unix
            )
        
        dfg.index.names=["log_datetime"]
                
        conn=get_conn("analysis")
        try:
            dfg.to_sql(con=conn, name="filtered_position_sensor_logs", 
                       if_exists="append", schema="analysisdb", 
                       method=insert_on_conflict_update)
        except AttributeError:
            print(dfg)

    
if __name__ == "__main__":

    import sys    
    try:
        run_regular_tilt_filter()
    except KeyboardInterrupt:
        print("Bye")
        sys.exit()
