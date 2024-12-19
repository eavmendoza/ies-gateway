import pandas as pd
from .dtdef import DateWindow, today
import argparse
import plotly.graph_objs as go
import configparser
import plotly.offline as py
import plotly.express as px
import numpy as np
import math
from plotly.subplots import make_subplots
from threading import Thread
import datetime

def get_gateway_loggers(bu_id:int) -> list:
    query = """SELECT r.id as sensor_id,r.code as code_name, r.name as sensor_name,
               b.id as bu_id,b.name as bu_name,
               s.name as site_name,s.code as site_code, s.id as site_id, l.id as logger_id 
               FROM power_sensors r
               inner join loggers l on r.logger_id = l.id
               inner join sites s on l.site_id = s.id
               inner join business_units b on s.business_unit_id = b.id
               where b.id={b_unit_id}
               and r.name like '%%gateway%%'
               and r.date_deactivated is null
               """.format(b_unit_id=bu_id)


    df_lgrs = pd.read_sql(query, props_db_conn)

    loggers = []
    for l in df_lgrs.values.tolist():
        lgr = {"pid":l[0], "nid":l[8], "site":l[5], "bu":l[4], "code":l[6]}
        loggers.append(lgr)

    return loggers

def get_network_status_logs(lgr:dict, window:DateWindow) -> pd.DataFrame:
    query = f"""SELECT log_datetime, net_stat 
               FROM gateway_network_status_logs 
               where logger_id={lgr['nid']} 
               and log_datetime>'{window.start.string}' and 
               log_datetime<'{window.end.string} 23:59:59'
               """

    df_n = pd.read_sql(query, logs_db_conn)

    df_n = df_n.set_index("log_datetime")
    # df_n = df_n.resample('10min', label='right').first()
    df_n = df_n.resample('10min').first()
    # print(df_n)
    df_n["net_stat"].fillna(-1,inplace=True)

    df_n = df_n.reset_index()
    
    return df_n

def get_power_logs(lgr:dict, window:DateWindow) -> pd.DataFrame:
    query = f"""SELECT log_datetime, voltage FROM power_sensor_logs 
            where power_sensor_id = {lgr['pid']} and log_datetime>'{window.start.string}' 
            and log_datetime<'{window.end.string} 23:59:59' 
            and voltage>10 and voltage<16 
            order by log_datetime"""

    return pd.read_sql(query, logs_db_conn)

def get_plots_network(lgr:dict, dfp, dfn):
    fig=go.Figure()

    trace=""
    
    try:
        trace = go.Scatter(x=dfp.log_datetime, y=dfp.voltage, name="Gateway Voltage")
    except AttributeError:
        dfp = dfp.reset_index()
        trace = go.Scatter(x=dfp.log_datetime, y=dfp.voltage, name="Gateway Voltage")
    
    fig.add_trace(trace)
        

    # dfn=dfn.set_index('log_datetime').resample('10min', label='right').first().reset_index()
    dfn=dfn.reset_index()

    start_zero_dt=None
    end_zero_dt=None
    for index, row in dfn.iterrows():
        if row.net_stat!=1 and start_zero_dt==None:
            start_zero_dt=row.log_datetime
            continue
        
        if (row.net_stat==1 or index==dfn.index[-1]) and start_zero_dt is not None:
            fig.add_vrect(x0=start_zero_dt, x1=row.log_datetime, 
              fillcolor="red", opacity=0.25, line_width=0)
            start_zero_dt=None
            
    fig.update_layout(
        title_text=f"Network vs Battery Voltage: <br>{lgr['bu']}-{lgr['site']}",
        legend=dict(orientation="h",yanchor="bottom",y=1.02,xanchor="right",x=1),
        xaxis_title = "Log Timestamp",
        yaxis_title = "Gateway Voltage",

    )
    
    return fig

def get_total_sampling_points(window:DateWindow) -> int:
    diff = window.end.dt-window.start.dt
    total_sampling_points = (diff.days+1)*24*6

    return total_sampling_points

def update_network_dash(bu_id:int, window:DateWindow, config:dict, real_time:bool):
    print("\nUpdating network and battery dashboard..")

    plots_dir=config['data_dir']['plots']
    data_dir=config['data_dir']['data']
    dash_dir=config['data_dir']['dash']

    loggers = get_gateway_loggers(bu_id)  
    # print(loggers) 
    df_ns=[] 
    dashboard_graphs=open(f"{dash_dir}/{BU_NAME[bu_id]}_DASHBOARD_net.html",'w')
    dashboard_graphs.write(f"<html><head><title>Network - {BU_NAME[bu_id]}</title></head><body>\n")
    
    total_sampling_points = get_total_sampling_points(window)
    logger_uptimes = []
    network_uptimes = []
    site_names = []
    
    dates = get_default_dates(real_time)
    
    for lgr in loggers:
        site_names.append(lgr["site"])
        df_p_zero = pd.DataFrame(columns=['voltage'], index=dates)
        df_n_zero = pd.DataFrame(columns=['net_stat'], index=dates)
        try:
            df_n = get_network_status_logs(lgr, window)
            # df_ns.append({"df":df_n, "site":lgr["site"], "code":lgr["code"]})
            df_n = df_n.set_index("log_datetime")
            df_n = df_n.resample('10min', label='right').first()
            network_uptimes.append(df_n[df_n.net_stat==1.0].net_stat.count()/total_sampling_points*100)
            df_n_zero.update(df_n, join='left')
        except (ValueError, TypeError):
        # except KeyboardInterrupt:
            print("No network logs available for {sensor_name}".format(sensor_name=lgr["site"]))
            network_uptimes.append(0)
        finally:
            df_n = df_n_zero
            df_n.index.name = "log_datetime"

        try:
            df_p = get_power_logs(lgr, window)
            print(lgr)
            df_pu = df_p.set_index("log_datetime")
            df_pu = df_pu.resample('10min', label='right').first()               
            df_p_zero.update(df_pu, join='left')
            logger_uptimes.append(df_pu[df_pu.notnull()].voltage.count()/total_sampling_points*100)
        except (ValueError, TypeError):
        # except KeyboardInterrupt:
            print("No power logs available for {sensor_name}".format(sensor_name=lgr["site"]))
            # fig = get_plots_network(lgr,df_p,df_n)
            logger_uptimes.append(0)
        finally:
            df_p = df_p_zero
            df_p.index.name = "log_datetime"
        
        fig = get_plots_network(lgr,df_p,df_n)
        fname = 'net_' + lgr["site"] + '.html'
        py.plot(fig, filename=f"{plots_dir}/{fname}",auto_open=False)
        dashboard_graphs.write(f"  <object data=\"plots_sb\\{fname}\" width=\"600\" height=\"500\"></object>\n")

    df = pd.DataFrame({'Logger Uptime': logger_uptimes, 'Network Uptimes': network_uptimes}, 
        index=site_names)

    # df = pd.DataFrame({'Sites':site_names, 'Uptime':uptimes})
    # ax = px.bar(df, x="Sites", y="Uptime", color="Uptime", 
    #             range_color=[0,100], title="Uptime in (%)")
    # ax = df.plot.bar(rot=0)

    fig = go.Figure(data=[
        go.Bar(name='Logger Uptimes', x=site_names, y=logger_uptimes),
            # marker=dict(color=logger_uptimes,colorscale='inferno',line=dict(cmin=0,cmax=100,width=2))),
        go.Bar(name='Network Uptimes', x=site_names, y=network_uptimes),
            # marker=dict(color=network_uptimes,colorscale='inferno',line=dict(cmin=0,cmax=100)))
    ])
    # Change the bar mode
    fig.update_layout(barmode='group', bargroupgap=0.2)
    # fig.update_coloraxes(showscale=True)
    fig.update_layout(
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        title=dict(
            text="Logger and Network Uptime in Percent"
        )
    )
    fig.show()
#     ax.show()
    net_uptime_fname = f"net_{BU_NAME[bu_id]}_uptime.html"
    py.plot(fig, filename=f"{plots_dir}/{net_uptime_fname}",auto_open=False)
    
    dashboard_graphs.write(f"  <object data=\"plots_sb\\{net_uptime_fname}\" width=\"600\" height=\"500\"></object>\n")
    dashboard_graphs.write("</body></html>")
    dashboard_graphs.close()


def update_rainfall_dash(bu_id:int, window:DateWindow, config:dict, real_time:bool):
    print("Updating rainfall dashboard..")

    plots_dir=config['data_dir']['plots']
    data_dir=config['data_dir']['data']
    dash_dir=config['data_dir']['dash']

    df_rain_sensors = get_rainfall_sensors(bu_id)  

    dashboard_graphs=open(f"{dash_dir}/{BU_NAME[bu_id]}_DASHBOARD_rain.html",'w')
    dashboard_graphs.write(f"<html><head><title>Rain - {BU_NAME[bu_id]}</title></head><body>\n")

    for index,s in df_rain_sensors.iterrows():
        try:
            df_ps = get_rainfall_logs(s.sensor_id, window)
            processed_rainfall_data = process_rain_logs(df_ps)
            fig = get_plots_rain(processed_rainfall_data, s)
            fname = f"rain_{str(s.sensor_id)}.html"
            py.plot(fig, filename=f"{plots_dir}/{fname}",auto_open=False)
            dashboard_graphs.write(f"  <object data=\"plots_sb\\{fname}\" width=\"600\" height=\"500\"></object>\n")
        except TypeError:
            print(f"No figure available for {s.site_name} {s.sensor_name}")
            continue

def get_rainfall_sensors(bu_id:int) -> pd.DataFrame:
    query = f"""SELECT r.id as sensor_id,r.name as sensor_name,b.id as bu_id,b.name as bu_name,
                s.name as site_name,s.code as site_code, s.id as site_id, l.id as logger_id 
                FROM rain_gauge_sensors  r  
                inner join loggers l on r.logger_id = l.id
                inner join sites s on l.site_id = s.id
                inner join business_units b on s.business_unit_id = b.id
                where b.id = {bu_id}
                and r.date_deactivated is null;"""


    return pd.read_sql(query, props_db_conn)

def get_rainfall_logs(s_id:int, window:DateWindow) -> pd.DataFrame:
    query = f"""SELECT log_datetime, num_ticks*0.254 as rain
                from rain_gauge_sensor_logs where rain_gauge_sensor_id = {s_id} 
                and log_datetime>'{window.start.string}'"""
    
    if window.end.string:
        query += f" and log_datetime < '{window.end.string} 23:59:59' "
        
    return pd.read_sql(query, logs_db_conn)

def process_rain_logs(df_ps:pd.DataFrame) -> dict:
    payload = {}

    df_ps = df_ps.set_index("log_datetime")
    df_ps = df_ps.resample('10min', label='right').first()*0.25
    df_p = df_ps.fillna(0.0).round(2)

    df_1h =df_p.rolling(window=6, min_periods=1).sum().round(2)
    df_1d =df_p.rolling(window=6*24, min_periods=1).sum().round(2)
    df_3d =df_p.rolling(window=6*24*3, min_periods=1).sum().round(2)
    
    df_all = pd.concat([df_1h, df_1d, df_3d], axis=1)

    payload['1h']=df_1h
    payload['1d']=df_1d
    payload['3d']=df_3d
    payload['all']=df_all

    return payload


def get_plots_rain(rain_data, sensor) -> go.Figure:

    # df_ps = df_ps.set_index("log_datetime")
    # df_ps = df_ps.resample('10min', label='right').first()
    # df_p = df_ps.fillna(0.0).round(2)

    # df_1h =df_p.rolling(window=6, min_periods=1).sum().round(2)
    # df_1d =df_p.rolling(window=6*24, min_periods=1).sum().round(2)
    # df_3d =df_p.rolling(window=6*24*3, min_periods=1).sum().round(2)
    
    # df_rain = pd.concat([df_1h, df_1d, df_3d], axis=1)

        
    df_1h = rain_data['1h'].reset_index()
    df_1d = rain_data['1d'].reset_index()
    df_3d = rain_data['3d'].reset_index()
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        mode="lines+markers",
        x=df_3d.log_datetime,
        y=df_3d.rain, name="3d")
    )
    
    fig.add_trace(go.Scatter(
        mode="lines+markers",
        x=df_1d.log_datetime,
        y=df_1d.rain, name="1d")
    )
    
    fig.add_trace(go.Scatter(
        mode="lines+markers",
        x=df_1h.log_datetime,
        y=df_1h.rain, name="1h")
    )
    
    fig.update_layout(
        title_text=f"{sensor.site_name.item()} - {sensor.sensor_name.item()} Cumulative Rainfall (mm)",
        legend=dict(orientation="h",yanchor="bottom",y=1.02,xanchor="right",x=1),
    )
    
    return fig

def get_plots_soms(df_list, n_soms) -> go.Figure:
    fig = go.Figure()
    clr = get_color("phase", np.linspace(0, 1, n_soms+1))
    counter=0
    for s in df_list:
        try:
            name=s["name"].split(' ')[2]+' '+s["site"]
        except IndexError:
            name=s["name"]


        fig.add_trace(go.Scatter(
            mode="lines+markers",
            x=s["df"].log_datetime,
            y=s["df"].ratio, name=name,
            marker={'color':clr[counter]}
            )
        )
        counter+=1 
        
    fig.update_layout(title_text="VWC",legend=dict(orientation="h",yanchor="bottom",y=1.02,xanchor="right",x=1),
                     coloraxis_autocolorscale=False)  
    
    return fig

def get_tilt_sensors(bu_id:int) -> pd.DataFrame:
    query = f"""SELECT r.id as sensor_id,r.name as sensor_name,b.id as bu_id,b.name as bu_name,
                s.name as site_name,s.code as site_code, s.id as site_id, l.id as logger_id 
                FROM position_sensors  r  
                inner join loggers l on r.logger_id = l.id
                inner join sites s on l.site_id = s.id
                inner join business_units b on s.business_unit_id = b.id
                where b.id = {bu_id}
                and r.date_deactivated is null;"""


    return pd.read_sql(query, props_db_conn)

def get_tilt_logs(s_id:int, window:DateWindow) -> pd.DataFrame:
    query = f"""SELECT log_datetime, ax_x as x, ax_y as y, ax_z as z 
                from position_sensor_logs where position_sensor_id = {s_id}     
                and log_datetime>'{window.start.string}'"""
    
    if window.end.string:
        query += f" and log_datetime < '{window.end.string} 23:59:59' "
        
    return pd.read_sql(query, logs_db_conn)

def orthogonal_filter(df:pd.DataFrame):

    # remove all non orthogonal value
    dfo = df.loc[:,('x','y','z')]/1
    mag = (dfo.x*dfo.x + dfo.y*dfo.y + dfo.z*dfo.z).apply(np.sqrt)
    lim = .2
    
    return df.loc[((mag>(1-lim)) & (mag<(1+lim)))]


def outlier_filter(dff):
    # dff = df.copy()

    dfmean = dff.loc[:,('x','y','z')].rolling(min_periods=1,window=96,center=False).mean()
    dfsd = dff.loc[:,('x','y','z')].rolling(min_periods=1,window=96,center=False).std()
    
    #setting of limits
    dfulimits = dfmean + (3*dfsd)
    dfllimits = dfmean - (3*dfsd)

    dff.loc[(dff.x > dfulimits.x) | (dff.x < dfllimits.x),'x'] = np.nan
    dff.loc[(dff.y > dfulimits.y) | (dff.y < dfllimits.y),'y'] = np.nan
    dff.loc[(dff.z > dfulimits.z) | (dff.z < dfllimits.z),'z'] = np.nan
    
    dff = dff[df.notnull()]
   
    return dff

def out_of_bounds_filter(dff):
    # dff = df.copy()

    max_limit = 1.05
    
    dff.loc[(dff.x.abs() > 1) & (dff.x.abs() < max_limit), 'x'] = 0.9999
    dff.loc[(dff.y.abs() > 1) & (dff.y.abs() < max_limit), 'y'] = 0.9999
    dff.loc[(dff.z.abs() > 1) & (dff.z.abs() < max_limit), 'z'] = 0.9999

    dff.loc[(dff.x.abs() > max_limit), 'x'] = np.nan
    dff.loc[(dff.y.abs() > max_limit), 'y'] = np.nan
    dff.loc[(dff.z.abs() > max_limit), 'z'] = np.nan

    # dff.x.loc[(dff.x.abs() > 1) & (dff.x.abs() < max_limit)] = 0.9999
    # dff.y.loc[(dff.y.abs() > 1) & (dff.y.abs() < max_limit)] = 0.9999
    # dff.z.loc[(dff.z.abs() > 1) & (dff.z.abs() < max_limit)] = 0.9999
    
    # dff.x.loc[(dff.x.abs() > max_limit)] = np.nan
    # dff.y.loc[(dff.y.abs() > max_limit)] = np.nan
    # dff.z.loc[(dff.z.abs() > max_limit)] = np.nan
    
    # dff = dff[dff.notnull()]
   
    return dff

def process_tilt_data(df_ps:pd.DataFrame) -> pd.DataFrame:
    df_ps = orthogonal_filter(df_ps)
    df_ps = out_of_bounds_filter(df_ps)
    df_ps = df_ps.set_index("log_datetime")
    df_ps = np.arcsin(df_ps)*180/math.pi
    df_ps = df_ps.resample('10min', label='right').first()
#             df_p = df_ps.copy()
#             df_p = df_p.fillna(method="ffill")
    
    periods = 6*3
    # df_p = df_ps.fillna(method="ffill")    
    df_p = df_ps.ffill()   
    df_p = df_p.rolling(window=periods, min_periods=1).mean()
    df_p_v = df_p.copy()

    # df_p.loc[df_p_na.x==np.nan, 'x'] = np.nan
    # df_p.loc[df_p_na.x==np.nan, 'y'] = np.nan
    # df_p.loc[df_p_na.x==np.nan, 'z'] = np.nan

    df_ps = df_ps.shift(periods=periods)

    df_p.loc[pd.isna(df_ps.x), 'x'] = np.nan
    df_p.loc[pd.isna(df_ps.x), 'y'] = np.nan
    df_p.loc[pd.isna(df_ps.x), 'z'] = np.nan
    
    # 'r' resample only
    # 'f' with rolling and forward fill
    return {'r': df_p, 'f': df_p_v}

def process_velocity_data(df_p:pd.DataFrame) -> pd.DataFrame:
    df_vel = project(normalize(df_p))
    df_vel = df_vel - df_vel.shift(periods=6)
    df_p = df_p.loc[df_p.x.notnull()]
    df_p = df_p.reset_index()
    return df_p

def normalize(df:pd.DataFrame) -> pd.DataFrame:
    df['xn'] = df.x / np.sqrt(df.x*df.x + df.y*df.y + df.z*df.z)
    df['yn'] = df.y / np.sqrt(df.x*df.x + df.y*df.y + df.z*df.z)
    df['zn'] = df.z / np.sqrt(df.x*df.x + df.y*df.y + df.z*df.z)
    return df 

def project(df:pd.DataFrame) -> pd.DataFrame:
    df['down'] = np.arctan2(df.zn,(np.sqrt(df.xn**2 + df.yn**2)))
    df['across'] = np.arctan2(df.yn,(np.sqrt(df.xn**2 + df.zn**2)))

    return df

def get_plots_tilt(df:pd.DataFrame, sensor:pd.Series) -> go.Figure:
    fig = make_subplots(rows=3, cols=1,
                        shared_xaxes=True,
                        vertical_spacing=0.02)

    df = df.reset_index()
        
    trace = go.Scatter(
        mode="lines+markers",
        x=df.log_datetime,
        y=df.x, name="X")
    
    fig.add_trace(trace, row=1, col=1)

    fig.add_trace(go.Scatter(
        mode="lines+markers",
        x=df.log_datetime,
        y=df.y, name="Y",), row=2, col=1
    )

    fig.add_trace(go.Scatter(
        mode="lines+markers",
        x=df.log_datetime,
        y=df.z, name="Z"), row=3, col=1
    )


    fig.update_xaxes(title_text="Timestamp", row=3, col=1)
    fig.update_yaxes(tickformat = ".2f")

    fig.update_layout(
        title_text=f"{sensor.site_name} - {sensor.sensor_name} inclination WRT Horizontal",
        legend=dict(orientation="h",yanchor="bottom",y=1.02,xanchor="right",x=1),
    )
    
    return(fig)

def get_plots_vel(df:pd.DataFrame, sensor:pd.Series) -> go.Figure:
    fig = make_subplots(rows=2, cols=1,
                        shared_xaxes=True,
                        vertical_spacing=0.02)

    df = df.reset_index()

    fig.add_trace(go.Scatter(
        mode="lines+markers",
        x=df.log_datetime,
        y=df.down, name="Down"), row=1, col=1
    )


    fig.add_trace(go.Scatter(
        mode="lines+markers",
        x=df.log_datetime,
        y=df.across, name="Across",), row=2, col=1
    )


    fig.update_xaxes(title_text="Timestamp", row=3, col=1)

    fig.update_layout(
        title_text=f"{sensor.site_name} - {sensor.sensor_name} Tilt Velocity",
        legend=dict(orientation="h",yanchor="bottom",y=1.02,xanchor="right",x=1),
    )
    
    return(fig)

def get_default_dates(real_time:bool):
    if real_time:
        end_dt = datetime.datetime.today()
    else:
        end_dt = window.end.dt+datetime.timedelta(days=1)

    return pd.date_range(window.start.dt, end_dt, freq='10min')

def update_tilt_dash(bu_id:int, window:DateWindow, config:dict, real_time:bool):

    print("\nUpdating tilt dashboard..")

    plots_dir=config['data_dir']['plots']
    data_dir=config['data_dir']['data']
    dash_dir=config['data_dir']['dash']

    df_tilt_sensors = get_tilt_sensors(bu_id)

    dash_tilt=open(f"{dash_dir}/{BU_NAME[bu_id]}_DASHBOARD_tilt.html",'w')
    dash_tilt.write(f"<html><head><title>Tilt - {BU_NAME[bu_id]}</title></head><body>\n")
    
    dash_vel=open(f"{dash_dir}/{BU_NAME[bu_id]}_DASHBOARD_vel.html",'w')
    dash_vel.write("<html><head></head><body>\n") 

    total_sampling_points = get_total_sampling_points(window)
    tilt_uptimes=[]
    sensor_codes=[]
    
    dates = get_default_dates(real_time)
    
    for index,s in df_tilt_sensors.iterrows():
        # dates = pd.date_range(window.start.dt, window.end.dt, freq='10min')
        df_new = pd.DataFrame(columns=['x','y','z'], index=dates)
        try:
            df_ps = get_tilt_logs(s.sensor_id, window)
            if len(df_ps)==0:
                # print("No data for", s.sensor_name, df_ps)
                raise ValueError
            df_tilt = process_tilt_data(df_ps)
            df_new.update(df_tilt['r'], join='left')
            df_vel = process_velocity_data(df_tilt['f'])
            # tilt_uptimes.append(len(df_tilt['f']))
        except (TypeError, AttributeError, ValueError) as e:
            # print(f"No data available for {s.site_name} - {s.sensor_name}")
            # df_vel = process_velocity_data(df_new)
            # continue
            # print("tilt error")
            pass
        finally:
            df_tilt = df_new
            df_tilt.index.name = "log_datetime"

        try:
            fig = get_plots_tilt(df_tilt, s)
        
            fname = f"tilt_{str(s.sensor_id)}.html"
            py.plot(fig, filename=f"{plots_dir}/{fname}",auto_open=False)
            dash_tilt.write(f"  <object data=\"plots_sb\\{fname}\" width=\"600\" height=\"500\"></object>\n")
            
            # figv = get_plots_vel(df_vel.reset_index(), s)
            
            # fname_vel = f"tilt_vel_{str(s.sensor_id)}.html"
            # py.plot(figv, filename=f"{plots_dir}/{fname_vel}",auto_open=False)
            # dash_vel.write(f"  <object data=\"plots_sb\\{fname_vel}\" width=\"600\" height=\"500\"></object>\n")
            
        except (TypeError, AttributeError) as e:
        # except KeyboardInterrupt:
            # print(f"No figure available for {s.site_name} - {s.sensor_name}")
            continue

        try:
            figv = get_plots_vel(df_vel.reset_index(), s)
            
            fname_vel = f"tilt_vel_{str(s.sensor_id)}.html"
            py.plot(figv, filename=f"{plots_dir}/{fname_vel}",auto_open=False)
            dash_vel.write(f"  <object data=\"plots_sb\\{fname_vel}\" width=\"600\" height=\"500\"></object>\n")
            
        except (UnboundLocalError) as e:
            print(f"No vel figure available for {s.site_name} - {s.sensor_name}")
            continue

    dash_tilt.write("</body></html>")
    dash_tilt.close()
    
    dash_vel.write("</body></html>")
    dash_vel.close()

def get_soms_sensors(bu_id:int) -> pd.DataFrame:
    query = f"""SELECT r.id as sensor_id,r.name as sensor_name,b.id as bu_id,b.name as bu_name,
                s.name as site_name,s.code as site_code, s.id as site_id, l.id as logger_id 
                FROM soil_moisture_sensors  r  
                inner join loggers l on r.logger_id = l.id
                inner join sites s on l.site_id = s.id
                inner join business_units b on s.business_unit_id = b.id
                where b.id = {bu_id}
                and r.name not like "Soil\_Moisture%%"
                and r.date_deactivated is null;"""

    return pd.read_sql(query, props_db_conn)

def get_soms_logs(s_id:int, window:DateWindow) -> pd.DataFrame:
    ratio_upper_limit=1.0
    query = f"""SELECT log_datetime, ratio
                from soil_moisture_sensor_logs where soil_moisture_sensor_id = {s_id} 
                and abs(ratio)<{str(ratio_upper_limit)} and log_datetime>'{window.start.string}'"""
    
    if window.end.string:
        query += f" and log_datetime < '{window.end.string} 23:59:59' "
        
    return pd.read_sql(query, logs_db_conn)

def process_soms_data(df_ps:pd.DataFrame) -> pd.DataFrame:
    df_ps.log_datetime = pd.to_datetime(df_ps['log_datetime'], unit = 's')
    df_ps = df_ps.set_index("log_datetime")
    df_p = df_ps.resample('10min', label='right').first()
    df_p = df_p.reset_index()
    df_p.ratio = df_p.ratio*100
        
    return df_p

def get_continuous_color(colorscale, intermed):
    import plotly.colors
    from PIL import ImageColor
    """
    Plotly continuous colorscales assign colors to the range [0, 1]. This function computes the intermediate
    color for any value in that range.

    Plotly doesn't make the colorscales directly accessible in a common format.
    Some are ready to use:
    
        colorscale = plotly.colors.PLOTLY_SCALES["Greens"]

    Others are just swatches that need to be constructed into a colorscale:

        viridis_colors, scale = plotly.colors.convert_colors_to_same_type(plotly.colors.sequential.Viridis)
        colorscale = plotly.colors.make_colorscale(viridis_colors, scale=scale)

    :param colorscale: A plotly continuous colorscale defined with RGB string colors.
    :param intermed: value in the range [0, 1]
    :return: color in rgb string format
    :rtype: str
    """
    if len(colorscale) < 1:
        raise ValueError("colorscale must have at least one color")

    hex_to_rgb = lambda c: "rgb" + str(ImageColor.getcolor(c, "RGB"))

    if intermed <= 0 or len(colorscale) == 1:
        c = colorscale[0][1]
        return c if c[0] != "#" else hex_to_rgb(c)
    if intermed >= 1:
        c = colorscale[-1][1]
        return c if c[0] != "#" else hex_to_rgb(c)

    for cutoff, color in colorscale:
        if intermed > cutoff:
            low_cutoff, low_color = cutoff, color
        else:
            high_cutoff, high_color = cutoff, color
            break

    if (low_color[0] == "#") or (high_color[0] == "#"):
        # some color scale names (such as cividis) returns:
        # [[loc1, "hex1"], [loc2, "hex2"], ...]
        low_color = hex_to_rgb(low_color)
        high_color = hex_to_rgb(high_color)

    return plotly.colors.find_intermediate_color(
        lowcolor=low_color,
        highcolor=high_color,
        intermed=((intermed - low_cutoff) / (high_cutoff - low_cutoff)),
        colortype="rgb",
    )

def get_color(colorscale_name, loc):
    from _plotly_utils.basevalidators import ColorscaleValidator
    # first parameter: Name of the property being validated
    # second parameter: a string, doesn't really matter in our use case
    cv = ColorscaleValidator("colorscale", "")
    # colorscale will be a list of lists: [[loc1, "rgb1"], [loc2, "rgb2"], ...] 
    colorscale = cv.validate_coerce(colorscale_name)
    
    if hasattr(loc, "__iter__"):
        return [get_continuous_color(colorscale, x) for x in loc]
    return get_continuous_color(colorscale, loc)

def update_soms_dash(bu_id:int, window:DateWindow, config:dict, real_time:bool):
    print("\nUpdating soms dashboard..")

    plots_dir=config['data_dir']['plots']
    data_dir=config['data_dir']['data']
    dash_dir=config['data_dir']['dash']

    df_soms_sensors = get_soms_sensors(bu_id)

    df_list = []
    n_soms=0

    dashboard_graphs=open(f"{dash_dir}/{BU_NAME[bu_id]}_DASHBOARD_soms.html",'w')
    dashboard_graphs.write(f"<html><head><title>Soil Moisture - {BU_NAME[bu_id]}</title></head><body>\n")

    for index,s in df_soms_sensors.iterrows():
        try:
            df_ps = get_soms_logs(s.sensor_id, window)
            if len(df_ps)==0:
                print("No data for", s.sensor_name)
                continue
            df_p = process_soms_data(df_ps)
        except (TypeError, IndexError, ValueError) as e:
            print(f"No data available for {s.site_name} - {s.sensor_name}")
            continue

        df_list.append({"df": df_p, "name": s.sensor_name, "site": s.site_name})
        n_soms+=1

    
    fig = go.Figure()
    clr = get_color("phase", np.linspace(0, 1, n_soms+1))
    counter=0
    for s in df_list:
        fig.add_trace(go.Scatter(
            mode="lines+markers",
            x=s["df"].log_datetime,
            y=s["df"].ratio, name=s["name"].split(' ')[2]+' '+s["site"],
            marker={'color':clr[counter]}
            )
        )
        counter+=1        
    
    fig.update_layout(title_text="VWC",legend=dict(orientation="h",yanchor="bottom",y=1.02,xanchor="right",x=1),
                     coloraxis_autocolorscale=False)
    
    fname = f"soms_{BU_NAME[bu_id]}.html"
    # py.plot(fig, filename=f"{dash_dir}/{BU_NAME[bu_id]}_DASHBOARD_soms.html",auto_open=False)
    py.plot(fig, filename=f"{plots_dir}/{fname}",auto_open=False)
    dashboard_graphs.write(f"  <object data=\"plots_sb\\{fname}\" width=\"900\" height=\"1000\"></object>\n")
    # dashboard_graphs.write(f"  <object data=\"plots_sb\\{fname}\" width=\"900\" height=\"500\"></object>\n")
    dashboard_graphs.write("</body></html>")
    dashboard_graphs.close()

def get_voltage_sensors(bu_id:int) -> pd.DataFrame:
    query = f"""SELECT r.id as sensor_id,r.name as sensor_name,b.id as bu_id,b.name as bu_name,
                s.name as site_name,s.code as site_code, s.id as site_id, l.id as logger_id 
                FROM power_sensors  r  
                inner join loggers l on r.logger_id = l.id
                inner join sites s on l.site_id = s.id
                inner join business_units b on s.business_unit_id = b.id
                where b.id = {bu_id}
                and r.name like '%%tlt%%'
                and r.date_deactivated is null;"""

    return pd.read_sql(query, props_db_conn)

def get_voltage_logs(s_id:int, window:DateWindow) -> pd.DataFrame:
    query = f"""SELECT log_datetime, voltage
                from power_sensor_logs where power_sensor_id = {s_id} 
                and log_datetime>'{window.start.string}'"""
    
    if window.end.string:
        query += f" and log_datetime < '{window.end.string} 23:59:59' "
        
    return pd.read_sql(query, logs_db_conn)

def process_voltage(df_ps:pd.DataFrame) -> pd.DataFrame:
    df_ps = df_ps.set_index("log_datetime")
    df_ps=df_ps.resample('30T', label='right').first()            
    df_ps=df_ps.reset_index()

    return df_ps

def update_power_dash(bu_id:int, window:DateWindow, config:dict, real_time:bool):
    print("\nUpdating power dashboard..")

    dash_dir=config['data_dir']['dash']

    df_voltage_sensors = get_voltage_sensors(bu_id)

    fig = go.Figure()

    for index,s in df_voltage_sensors.iterrows():
        try:            
            df_ps = get_voltage_logs(s.sensor_id, window)
            df_ps = process_voltage(df_ps)
        except (TypeError, IndexError, ValueError) as e:
            print("No figure available for {site_name} - {sensor_name}".format(sensor_name=s.sensor_name, 
                                                                               site_name=s.site_code))
            continue 

        fig.add_trace(go.Scatter(
            mode="lines+markers",
            x=df_ps.log_datetime,
            y=df_ps.voltage, name=s.sensor_name,
            )
        )

    fig.update_layout(title_text="Sensor voltages",legend=dict(orientation="h",yanchor="bottom",y=1.02,xanchor="right",x=1),
                     coloraxis_autocolorscale=False)
    
    py.plot(fig, filename=f"{dash_dir}/{BU_NAME[bu_id]}_DASHBOARD_power.html",auto_open=False)
            
        
def get_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d","--days_span", help="days span")
    parser.add_argument("-e","--end_day", help="end day")
    parser.add_argument("-s","--start_day", help="start day")
    parser.add_argument("-b","--bu_id", help="business unit id", type=int)
    parser.add_argument("-c","--save_to_csv", help="save to csv", action='store_true')
    parser.add_argument("-t","--type", help="plot type")
    parser.add_argument("-r","--real_time", help="reflect real time", action='store_true')

    args = parser.parse_args()

    if not args.bu_id:
        # generate plots for all sites
        args.bu_id = list(range(1,5))
    else:
        args.bu_id = [args.bu_id]

    if not args.end_day:
        args.end_day = today()
        args.real_time = True
    else:
        args.real_time = False
        # print(args.end_day.dt)

    if not args.days_span and not args.end_day:
        args.days_span = 14

    if not args.type:
        args.type="all"


    return args

    
BU_NAME={1:"BGBU", 2:"LGBU", 3:"NIGBU", 4:"MAGBU"}




