import re
import logging
from logging.handlers import RotatingFileHandler
from sqlalchemy import create_engine
import mysql.connector
import pandas as pd
from datetime import datetime


def connsql(db_type="logs"):
    import yaml, sys
    from pathlib import Path as path
    # cred_file = str(path.home())+'/gateway/setup/db_cred.yaml'
    cred_file = '/home/ies/gateway/setup/db_cred.yaml'
    creds = yaml.load(open(cred_file))['DB']

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

def setup_logger():
    log_formatter = logging.Formatter('%(asctime)s %(levelname)s %(funcName)s(%(lineno)d) %(message)s')

    logFile = 'parser.log'

    my_handler = RotatingFileHandler(logFile, mode='a', maxBytes=5*1024*1024, 
                                     backupCount=2, encoding=None, delay=0)
    my_handler.setFormatter(log_formatter)
    app_log = logging.getLogger('root')
    app_log.setLevel(logging.INFO)
    app_log.addHandler(my_handler)

    return app_log

def get_unparsed_messages(limit):
    conn = connsql() 
    SQL_QUERY = """SELECT id, message FROM message_transactions 
    WHERE feed_name IS NOT NULL AND 
    parsed_datetime IS NULL AND 
    error_parse_datetime = '2010-01-01'
    ORDER BY transaction_datetime DESC 
    LIMIT {limit}""".format(limit=limit)
    df_res = pd.read_sql(SQL_QUERY, conn["logs"])
    conn["logs"].close()
    return df_res

def get_logger_props(msg, d_type, search_for):
    query = """SELECT r.id as sensor_id,r.code as code_name, l.id as logger_id """
    
    if (d_type=="tilt"):
        query += "FROM position_sensors r "
        search_tag = "where r.code = 'TLT{logger_code}'".format(logger_code=search_for.zfill(3))
    elif (d_type=="gateway_power"):
        query += "FROM power_sensors r "
        search_tag = "where r.id={logger_id}".format(logger_id=search_for)
    elif (d_type=="soil"):
        query += "FROM soil_moisture_sensors r "
        search_tag = "where r.code='SMS{logger_code}'".format(logger_code=search_for.zfill(3))
    elif (d_type=="sensor_power"):
        query += "FROM power_sensors r "
        search_tag = "where r.code='TLT{logger_code}'".format(logger_code=search_for.zfill(3))
    elif (d_type=="rain"):
        query += "FROM rain_gauge_sensors r "
        search_tag = "where r.id={logger_id}".format(logger_id=search_for)
    else:
        raise ValueError("Unknown data type {d_type}".format(d_type))
        
    query += """inner join loggers l on r.logger_id = l.id
        inner join sites s on l.site_id = s.id
        inner join business_units b on s.business_unit_id = b.id
        {search_tag}
        and r.date_deactivated is null;""".format(search_tag=search_tag)
    
    conn = connsql("props")    
    result = pd.read_sql(query, conn["props"])
    if len(result)==0:
        raise ValueError("No {d_type} logger found {search_for}".format(d_type=d_type, search_for=search_for))

    conn["props"].dispose()
    
    return result.iloc[0].to_dict()

def convert_datetime(dtm_str):
    from datetime import datetime
    
    try:
        return datetime.strptime(dtm_str, '%y%m%d%H%M%S').strftime("%Y-%m-%d %H:%M:%S")
    except:
        raise ValueError("Error converting datetime")

def sql_execute_commit(query, values):
    conn = connsql()    
    conn["logs"].cursor().execute(query, (values))
    conn["logs"].commit()
    conn["logs"].cursor().close()
    conn["logs"].close()
    

def insert_power_log(msg):
    try:
        # gateway power log
        SQL_INSERT = "INSERT IGNORE INTO power_sensor_logs (log_datetime, voltage, power, power_sensor_id, message_transaction_id) values (%s, %s, %s, %s, 1);"
        values = (msg["log_dt"], msg["msg"]["BV"], msg["msg"]["PW"], msg["msg"]["PI"])    
    except KeyError:
        # sensor power log
        SQL_INSERT = "INSERT IGNORE INTO power_sensor_logs (log_datetime, voltage, power_sensor_id, message_transaction_id) values (%s, %s, %s, 1);"
        values = (msg["log_dt"], msg["msg"]["BV"], int(msg["power_props"]["sensor_id"]))
    sql_execute_commit(SQL_INSERT, values)

def insert_tilt_log(msg):
    SQL_INSERT = "INSERT IGNORE INTO position_sensor_logs (log_datetime, ax_x, ax_y, ax_z, position_sensor_id, message_transaction_id) values (%s, %s, %s, %s, %s, 1);"
    values = (msg["log_dt"], float(msg["msg"]["AX"]), float(msg["msg"]["AY"]), float(msg["msg"]["AZ"]), int(msg["tilt_props"]["sensor_id"]))    
    # print(msg)
    sql_execute_commit(SQL_INSERT, values)
    
def insert_soil_log(msg):
    SQL_INSERT = "INSERT IGNORE INTO soil_moisture_sensor_logs (log_datetime, ratio, soil_moisture_sensor_id, message_transaction_id) values (%s, %s, %s, 1);"
    values = (msg["log_dt"], msg["msg"]["SO"], int(msg["soil_props"]["sensor_id"]))
    sql_execute_commit(SQL_INSERT, values)
    
def insert_rain_log(msg):
    SQL_INSERT = "INSERT IGNORE INTO rain_gauge_sensor_logs (log_datetime, num_ticks, rain_gauge_sensor_id, message_transaction_id) values (%s, %s, %s, 1);"
    values = (msg["log_dt"], int(msg["msg"]["TI"]), int(msg["rain_props"]["sensor_id"]))    
    sql_execute_commit(SQL_INSERT, values)

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

def get_arguments():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-l","--limit", help="max messages to process", type=int)

    args = parser.parse_args()

    if not args.limit:
        args.limit = 100

    return args

def update_message_parsed(message_id):
    SQL_UPDATE = """
        UPDATE message_transactions
        SET
            parsed_datetime = %s,
            error_parse_datetime=NULL
        WHERE
            id = %s
    """
    sql_execute_commit(SQL_UPDATE, (datetime.today(), message_id))

def record_unparsed_message(message_id):
    SQL_UPDATE = '''
        UPDATE message_transactions
        SET
            error_parse_datetime = %s
        WHERE
            id = %s
    '''
    sql_execute_commit(SQL_UPDATE, (datetime.today(), message_id))

def back_comp_msg(message):
    if re.search("[;,]DTM", message):
        message = re.sub(r'[;,]DTM', ',DT', message)
        
    # if re.search("LC:", message):
    #     message = re.sub(r'LC:', 'PI:', message)

    if re.search("VO:", message):
        message = re.sub(r'VO:', 'BV:', message)

    message = re.sub(r'NEG', 'NGR', message)

    return message

def parse_message(msg):
    bit_pairs = re.findall("[A-Z]{2}\:[-\.\d]+",msg)
    msg_dict = {'msg':dict()}

    for pair in bit_pairs:
        msg_dict["msg"][pair.split(":")[0]] = pair.split(":")[1]
        
    msg_dict["log_dt"] = convert_datetime(msg_dict["msg"]["DT"])
    msg_dict["msg_type"] = get_msg_type(msg_dict["msg"])
    # print(msg_dict)

    if msg_dict["msg_type"]=="power":
        msg_dict["power_props"] = get_logger_props(msg_dict,d_type="gateway_power",search_for=msg_dict["msg"]["PI"])
    #     insert_power_log(msg_dict)
    elif msg_dict["msg_type"]=="sensor":
        logger_code = msg_dict["msg"]["LC"]
        msg_dict["tilt_props"] = get_logger_props(msg_dict,d_type="tilt",search_for=logger_code)
        msg_dict["soil_props"] = get_logger_props(msg_dict,d_type="soil",search_for=logger_code)
        msg_dict["power_props"] = get_logger_props(msg_dict,d_type="sensor_power",search_for=logger_code)
    elif msg_dict["msg_type"]=="rain":
        msg_dict["rain_props"] = get_logger_props(msg_dict,d_type="rain",search_for=msg_dict["msg"]["RI"])
    else:
        log.error("Unknow message type")
        # print("message type error")

    return msg_dict
        
        
    # if "tilt_props" in msg_dict.keys():
    #     insert_tilt_log(msg_dict)
        
    # if "soil_props" in msg_dict.keys():
    #     insert_soil_log(msg_dict)
        
    # if "rain_props" in msg_dict.keys():
    #     insert_rain_log(msg_dict)
        
    # if "power_props" in msg_dict.keys():
    #     insert_power_log(msg_dict)

