# -*- coding: utf-8 -*-
"""
Created on Mon Jun 17 13:25:43 2024

@author: earlm
"""
import time
import pandas as pd
import re
from analysis.dtdef import DateWindow, today
import argparse
from analysis import process as proc
from analysis.process import get_plots_soms
from analysis.settings import get_config
import plotly.offline as py
from pathlib import Path

config_dir = str(Path(__file__).parent.absolute()) + "/spyder/"

sensor_props = dict()
sensor_props["tilt"] = pd.read_csv(config_dir+"tilt_props.csv")
sensor_props["soil"] = pd.read_csv(config_dir+"soil_props.csv")
sensor_props["rain"] = pd.read_csv(config_dir+"rain_props.csv")
sensor_props["power"] = pd.read_csv(config_dir+"power_props.csv")

data_lines_str = []

# df_msg = pd.DataFrame()
df_msg = []

def back_comp_msg(message):
    if re.search("LI", message):
        message = re.sub(r'LI', 'RI', message)
        
    if re.search("LC:132", message):
        message = re.sub(r'LC:132', 'LC:137', message)
        
    if re.search("LC:133", message):
        message = re.sub(r'LC:133', 'LC:138', message)
        
    if re.search("[;,]DTM", message):
        message = re.sub(r'[;,]DTM', ',DT', message)

    return message

def get_messages_from_dump_files(f_source):
    
    dump_file = open(f_source, 'r', encoding="utf8")
    # 
    # data_lines_str = []
    # df_msg = []
    
    for line in dump_file.readlines():
        msg=""
        try:
            msg = re.search(r"(?<=\()\d{4,6}.+(?=\)[,;])", line).group(0)
            msg = back_comp_msg(msg)
            # data_lines_str.append(msg)
            
        except AttributeError:
            continue
        
        # print(msg)s
        id_str = ""
        try:
            # dt_str = re.search("\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}",line).group(0)
            id_str = re.search("\d{4,10}(?=,)", msg).group(0)
        except AttributeError:
            continue
        
        dt_str= ""
        try:
            dt_str = re.search("\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}",line).group(0)
            # dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
        except AttributeError:
            continue
        
        msg_str=""
        try:
            # pattern = "(?<=\'"+dt_str+"\',\').+(?=\')"
            msg_str = re.search("(?<=\',\').+(?=\',)", msg).group(0)
        except AttributeError:
            continue
        
        df_msg.append({"id":id_str, "dt":dt_str, "message":msg_str})
        
        
    return pd.DataFrame(df_msg)

def parse_message(msg):
    bit_pairs = re.findall("[A-Z]{2}\:[-\.\d]+",msg)
    msg_dict = {'msg':dict()}

    for pair in bit_pairs:
        msg_dict["msg"][pair.split(":")[0]] = pair.split(":")[1]
        
    try:
        msg_dict["log_dt"] = convert_datetime(msg_dict["msg"]["DT"])
    except KeyError:
        print("Unknown Error:", msg)
        raise ValueError
    
    try:
        msg_dict["msg_type"] = get_msg_type(msg_dict["msg"])
    except ValueError:
        print("Unknown Error:", msg)
        raise ValueError


    if msg_dict["msg_type"]=="power":
        msg_dict["power_props"] = get_logger_props(msg_dict,d_type="gateway_power",search_for=msg_dict["msg"]["PI"])
    elif msg_dict["msg_type"]=="sensor":
        logger_code = msg_dict["msg"]["LC"]
        msg_dict["tilt_props"] = get_logger_props(msg_dict,d_type="tilt",search_for=logger_code)
        msg_dict["soil_props"] = get_logger_props(msg_dict,d_type="soil",search_for=logger_code)
        msg_dict["power_props"] = get_logger_props(msg_dict,d_type="sensor_power",search_for=logger_code)
    elif msg_dict["msg_type"]=="rain":
        msg_dict["rain_props"] = get_logger_props(msg_dict,d_type="rain",search_for=msg_dict["msg"]["RI"])
    # else:
    #     log.error("Unknown message type")
    
    return msg_dict        
    

def convert_datetime(dtm_str):
    from datetime import datetime
    
    try:
        return datetime.strptime(dtm_str, '%y%m%d%H%M%S').strftime("%Y-%m-%d %H:%M:%S")
    except:
        raise ValueError("Error converting datetime")
        
def get_msg_type(msg):
    msg_type_table_dict = {
        'PI': 'power',
        'RI': 'rain',
        'LC': 'sensor',
    }
    
    for key in msg_type_table_dict.keys():
        if key in msg.keys():
            return msg_type_table_dict[key]
            
    raise ValueError("Unknown message type {}".format(str(msg.keys())))
    
def get_logger_props(msg, d_type, search_for):
    result=""
    if (d_type=="tilt"):
        result = sensor_props["tilt"].query('sensor_name.str.contains("TLT{logger_code}")'.format(logger_code=search_for.zfill(3)))
    elif (d_type=="soil"):
        result = sensor_props["soil"].query('sensor_name.str.contains("SMS{logger_code}")'.format(logger_code=search_for.zfill(3)))
    elif (d_type=="sensor_power"):
        result = sensor_props["power"].query('sensor_name.str.contains("TLT{logger_code}")'.format(logger_code=search_for.zfill(3)))
    elif (d_type=="rain"):
        result = sensor_props["rain"].loc[sensor_props["rain"]["sensor_id"]==int(search_for)]
    else:
        raise ValueError("Unknown data type {d_type}".format(d_type))

    
    return {key: result[key] for key in result.keys()}


def get_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d","--days_span", help="days span")
    parser.add_argument("-e","--end_day", help="end day")
    parser.add_argument("-s","--start_day", help="start day")
    parser.add_argument("-b","--bu_id", help="business unit id", type=int)
    parser.add_argument("-c","--save_to_csv", help="save to csv", action='store_true')
    parser.add_argument("-t","--type", help="plot type")
    parser.add_argument("-r","--real_time", help="reflect real time", action='store_true')
    parser.add_argument("-f","--file", help="reflect real time", type=str)

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
        
    if not args.file:
        raise ValueError("No file specified")


    return args
    

if __name__ == "__main__":

    print("Processing data..")
    
    args=get_arguments()
    if args.days_span:
        days_span=int(args.days_span)
    else:
        days_span=None
    window = DateWindow(days_span=days_span, end_day=args.end_day, start_day=args.start_day)
    
    print("Reading database dump file..")
   
    messages = get_messages_from_dump_files(args.file)
    messages['dt'] = pd.to_datetime(messages['dt'], format='%Y-%m-%d %H:%M:%S')

    
    messages = messages.loc[(messages['dt'] > window.start.dt)]
    
    messages = messages.drop(["dt","id"], axis=1)
    # print(messages)
    
    tilt_data_ls = []
    soil_data_ls = []
    sensor_power_data_ls = []
    rain_data_ls = []    
    gateway_power_data_ls = []
    
    start_time = time.time()
    
    print("Parsing raw data..")
    
    for index, row in messages.iterrows():
        
        try:
            msg = parse_message(row.message)
        except (ValueError, KeyError): # to do: parse gateway poawer
            continue

        try:        
            if "tilt_props" in msg.keys():
                tilt_data_ls.append({
                    "log_datetime": msg["log_dt"], 
                    "x":float(msg["msg"]["AX"]), 
                    "y":float(msg["msg"]["AY"]), 
                    "z":float(msg["msg"]["AZ"]), 
                    "tilt_sensor_id":int(msg["tilt_props"]["sensor_id"].iloc[0])
                })
        except IndexError:
            pass
            
        try:
            if "soil_props" in msg.keys():
                soil_data_ls.append({
                    "log_datetime": msg["log_dt"], 
                    "ratio":float(msg["msg"]["SO"]), 
                    "soil_moisture_sensor_id":int(msg["soil_props"]["sensor_id"].iloc[0])
                })
        except IndexError:
            pass
            
        try:
            if "rain_props" in msg.keys():
                rain_data_ls.append({
                    "log_datetime": msg["log_dt"], 
                    "rain":int(msg["msg"]["TI"]),
                    "rain_gauge_sensor_id":int(msg["rain_props"]["sensor_id"].iloc[0])
                })
        except IndexError:
            pass
            
        try:
            if "power_props" in msg.keys() and "PW" in msg["msg"].keys():
                gateway_power_data_ls.append({
                    "log_datetime": msg["log_dt"], 
                    "voltage":msg["msg"]["BV"],
                    "power":msg["msg"]["PW"],
                    "power_sensor_id":int(msg["power_props"]["sensor_id"].iloc[0])
                })
        
            
            elif "power_props" in msg.keys() and "BV" in msg["msg"].keys():
                sensor_power_data_ls.append({
                    "log_datetime": msg["log_dt"], 
                    "voltage":msg["msg"]["BV"],
                    "power_sensor_id":int(msg["power_props"]["sensor_id"].iloc[0])
                })
        except IndexError:
            pass
            
    
    
    config = get_config()
    plots_dir=config['data_dir']['plots'].replace("\"","")
    data_dir=config['data_dir']['data']
    dash_dir=config['data_dir']['dash']
    
    # print(data_dir)
    
    # dates = proc.get_default_dates(real_time)
            
    print("Generating charts..")
    
    if len(tilt_data_ls)>0:
        df_tilt_all = pd.DataFrame(tilt_data_ls)
    
        for sensor_id in df_tilt_all["tilt_sensor_id"].unique():
            df_tilt = df_tilt_all.loc[df_tilt_all["tilt_sensor_id"]==sensor_id].drop("tilt_sensor_id", axis=1)
            df_tilt["log_datetime"] = pd.to_datetime(df_tilt["log_datetime"], format='%Y-%m-%d %H:%M:%S')
            df_tilt = proc.process_tilt_data(df_tilt)
            sensor_name = sensor_props["tilt"].loc[sensor_props["tilt"]["sensor_id"]==sensor_id]["sensor_name"].item()
            df_tilt['r'].to_csv(f"{data_dir}tilt_{sensor_name}.csv", float_format='%.3f')
            # df_new.update(df_tilt['r'], join='left')
            
            fig = proc.get_plots_tilt(df_tilt['r'], sensor_props["tilt"].loc[sensor_props["tilt"]["sensor_id"]==sensor_id])
            fname = f"tilt_{str(sensor_id)}.html"
            py.plot(fig, filename=f"{plots_dir}{fname}",auto_open=False)
        
    
    if len(soil_data_ls)>0:
        df_soil_all = pd.DataFrame(soil_data_ls)
        df_list = []
        n_soms=0
        for sensor_id in df_soil_all["soil_moisture_sensor_id"].unique():
            df_soil = df_soil_all.loc[df_soil_all["soil_moisture_sensor_id"]==sensor_id].drop("soil_moisture_sensor_id", axis=1)
            df_soil["log_datetime"] = pd.to_datetime(df_soil["log_datetime"], format='%Y-%m-%d %H:%M:%S')
            df_soil = proc.process_soms_data(df_soil)
            # df_new.update(df_soil['r'], join='left')
            sensor_name = sensor_props["soil"].loc[sensor_props["soil"]["sensor_id"]==sensor_id]["sensor_name"].item()
            df_soil.to_csv(f"{data_dir}soil_{sensor_name}.csv", index=False, float_format='%.1f')
            
            df_list.append({
                "df": df_soil, 
                "name": sensor_name, 
                "site": "This station"
            })
            n_soms+=1
            
        fig = get_plots_soms(df_list, n_soms)
        fname = f"soil_moisture.html"
        py.plot(fig, filename=f"{plots_dir}{fname}",auto_open=False)   
    
    
    if len(rain_data_ls)>0:
        df_rain_all = pd.DataFrame(rain_data_ls)
                    
        for sensor_id in df_rain_all["rain_gauge_sensor_id"].unique():
            # df_new = pd.DataFrame(columns=['x','y','z'], index=dates)
            df_rain = df_rain_all.loc[df_rain_all["rain_gauge_sensor_id"]==sensor_id].drop("rain_gauge_sensor_id", axis=1)
            
            df_rain["log_datetime"] = pd.to_datetime(df_rain["log_datetime"], format='%Y-%m-%d %H:%M:%S')
            dict_rain = proc.process_rain_logs(df_rain)
            
            sensor_name = sensor_props["rain"].loc[sensor_props["rain"]["sensor_id"]==sensor_id]["sensor_id"].item()
            dict_rain['all'].columns=['1hour', '1day', '3day']
            dict_rain['all'].to_csv(f"{data_dir}rain_RG{sensor_name}.csv", float_format='%.2f')
            
            
            fig = proc.get_plots_rain(dict_rain, sensor_props["rain"].loc[sensor_props["rain"]["sensor_id"]==sensor_id])
            fname = f"rain_{str(sensor_id)}.html"
            py.plot(fig, filename=f"{plots_dir}{fname}",auto_open=False)
        
        
    print("done")