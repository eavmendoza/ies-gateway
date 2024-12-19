import mysql.connector as mysql_connector
from logging.handlers import RotatingFileHandler
from datetime import datetime
import re
import sys
import logging

class MessageTransaction:
  def __init__(self, message_id, message, bu_code=None, site_code=None, 
               sensor_code=None, logger_id=None, data_field=None, 
               info_field=None, data_dict=None, txn_datetime=None):
    self.bu_code = bu_code
    self.site_code = site_code
    self.logger_id = logger_id
    self.sensor_code = sensor_code
    self.data_field = data_field
    self.info_field = info_field
    self.message_id = message_id
    self.data_dict = data_dict
    self.message = message
    self.txn_datetime = txn_datetime

def get_credentials_dict():
    import yaml
    # credentials_dict = yaml.load(open('/home/ies/gateway/parser/credentials.yaml'))
    credentials_dict = yaml.load(open('/home/ies/gateway/setup/db_cred.yaml'))

    return credentials_dict


def get_db_credentials():
    credentials_dict = get_credentials_dict()

    db_credentials = credentials_dict['DB']

    host = db_credentials['Host']
    db_name = db_credentials['DbName']
    user = db_credentials['User']
    password = db_credentials['Password']

    return host, db_name, user, password

    return credentials_dict

def connect_db():
    host, db_name, user, password = get_db_credentials()

    return mysql_connector.connect(host=host,
                                   db=db_name,
                                   user=user,
                                   passwd=password, auth_plugin='mysql_native_password')


def get_unparsed_messages(db_cursor, limit):

    SQL_QUERY = """
        SELECT
            id,
            message,
            transaction_datetime
        FROM
            message_transactions
        WHERE
            parsed_datetime IS NULL AND
            error_parse_datetime IS NULL
        ORDER BY
            transaction_datetime DESC
            limit {limit}
    """.format(limit=limit)

    db_cursor.execute(SQL_QUERY)

    messages = db_cursor.fetchall()

    return messages


def convert_dtm_datetime(dtm_value):
    return datetime.strptime(dtm_value, '%y%m%d%H%M%S')

def convert_data_dict(data_message):

    # data = re.search("(?<=\$).+(?=\$)",data_message).group(0)
    # data = re.search("(?<=\$).+(?=\$)",data_message).group(0)
    key_value_pairs = re.findall("[A-Za-z_0-9]+\:[0-9\.\- ,]+",data_message)
    
    data_dict = {}
    for piece in key_value_pairs:
        [key,val] = piece.split(":")
        data_dict[key] = val
        
    data_dict['DTM'] = convert_dtm_datetime(data_dict['DTM'])

    return data_dict

def get_bu_site_sensor_code(location_message):

    location_code_list = location_message.split('-')

    # if len(location_code_list) != 3:
    #     raise Exception('Location Codes is less than or more than 3.')
    
    return location_code_list


def get_logger_sensor_id(db_cursor, business_unit_code, site_code, sensor_code):

    sensor_type_table_dict = {
        'SMS': 'soil_moisture_sensors',
        'Soil': 'soil_moisture_sensors',
        'TLT': 'position_sensors',
        'RG': 'rain_gauge_sensors',
        'Rain': 'rain_gauge_sensors',
        'SLR': 'power_sensors',
        'GTW': 'power_sensors',
        'Tilt': 'gkn_tilt_sensors'
    }

    if site_code == "LPR":
        site_code = "LRD"

    sensor_table = None

    for key in sensor_type_table_dict.keys():
        if key in sensor_code:
            sensor_table = sensor_type_table_dict[key]
            
#     print("sensor table =", sensor_table)
#     print("sensor code =", sensor_code)

    if sensor_table is None:
        raise Exception('No Table found for this Sensor Code.')

    SQL_QUERY = """
        SELECT
            logger.id,
            sensor.id
        FROM
            business_units AS bu,
            sites AS site,
            loggers AS logger,
            {sensor_table} AS sensor
        WHERE
            bu.id = site.business_unit_id AND
            site.id = logger.site_id AND
            logger.id = sensor.logger_id AND
            bu.code = '{bu}' AND
            site.code = '{site}' AND
            sensor.code = '{sc}'
    """.format(sensor_table=sensor_table, bu=business_unit_code,
              site=site_code, sc=sensor_code)
    
    db_cursor.execute(SQL_QUERY)

    logger_sensor_id_list = db_cursor.fetchall()

    if len(logger_sensor_id_list) > 0:
        logger_id = logger_sensor_id_list[0][0]
        sensor_id = logger_sensor_id_list[0][1]

        return logger_id, sensor_id

    else:
        raise Exception('No Sensor ID found in Database.')

def update_message_parsed(db_cursor, message_id):
    # Record the time the message has been parsed.
    # This will also place the message to be not parsed on future parsing jobs

    SQL_UPDATE = """
        UPDATE message_transactions
        SET
            parsed_datetime = %s
        WHERE
            id = %s
    """

    db_cursor.execute(SQL_UPDATE, (datetime.today(), message_id,))

def parse_rain_gauge_data(db_cursor, data_dict, sensor_id, message_id):

    if 'PER' in data_dict.keys():
        # Has a period

        SQL_INSERT = """
            INSERT IGNORE INTO rain_gauge_sensor_logs
            SET
                log_datetime = %s,
                num_ticks = %s,
                period = %s,
                rain_gauge_sensor_id = %s,
                message_transaction_id = %s
        """

        db_cursor.execute(SQL_INSERT, (data_dict['DTM'], data_dict['TIP'], data_dict['PER'], sensor_id, message_id,))
    elif 'INS' in data_dict.keys():
        # Does not have a period

        SQL_INSERT = """
            INSERT IGNORE INTO rain_gauge_sensor_logs
            SET
                log_datetime = %s,
                num_ticks = %s,
                rain_gauge_sensor_id = %s,
                message_transaction_id = %s
        """
        db_cursor.execute(SQL_INSERT, (data_dict['DTM'], data_dict['INS'], sensor_id, message_id,))

    update_message_parsed(db_cursor, message_id)


def parse_soil_moisture_data(db_cursor, data_dict, sensor_id, message_id):

    SQL_INSERT = """
        INSERT IGNORE INTO soil_moisture_sensor_logs
        SET
            log_datetime = %s,
            ratio = %s,
            soil_moisture_sensor_id = %s,
            message_transaction_id = %s
    """

    try:
        ratio = data_dict['VWC']
    except KeyError:
        ratio = data_dict[list(data_dict.keys())[0]]
        
    db_cursor.execute(SQL_INSERT, (data_dict['DTM'], ratio, sensor_id, message_id,))

    update_message_parsed(db_cursor, message_id)


def parse_power_sensor_data(db_cursor, data_dict, sensor_id, message_id):

    if 'BTP' in data_dict.keys():
        # Has a power data

        SQL_INSERT = """
            INSERT IGNORE INTO power_sensor_logs
            SET
                log_datetime = %s,
                voltage = %s,
                current = %s,
                power = %s,
                power_sensor_id = %s,
                message_transaction_id = %s
        """
        db_cursor.execute(SQL_INSERT, (data_dict['DTM'], data_dict['BTV'], data_dict['BTA'], data_dict['BTP'], sensor_id, message_id,))

    else:
        # Has no power data

        SQL_INSERT = """
            INSERT IGNORE INTO power_sensor_logs
            SET
                log_datetime = %s,
                voltage = %s,
                current = %s,
                power_sensor_id = %s,
                message_transaction_id = %s
        """
        db_cursor.execute(SQL_INSERT, (data_dict['DTM'], data_dict['BTV'], data_dict['BTA'], sensor_id, message_id,))

    update_message_parsed(db_cursor, message_id)

def split_accel_data(data):
    data_list = data.split(',')

    return data_list[0], data_list[1], data_list[2]


def parse_position_sensor_data(db_cursor, data_dict, sensor_id, message_id):

    accel_x, accel_y, accel_z = split_accel_data(data_dict['AXL'])
    mag_x, mag_y, mag_z = split_accel_data(data_dict['MGR'])

    SQL_INSERT = """
        INSERT IGNORE INTO position_sensor_logs
        SET
            log_datetime = %s,
            ax_x = %s, ax_y = %s, ax_z = %s,
            mag_x = %s, mag_y = %s, mag_z = %s,
            position_sensor_id = %s,
            message_transaction_id = %s
    """

    db_cursor.execute(SQL_INSERT, (
        data_dict['DTM'], 
        accel_x, accel_y, accel_z,
        mag_x, mag_y, mag_z,
        sensor_id,
        message_id,))

    update_message_parsed(db_cursor, message_id)

def parse_gkn_rain_sensor_data(db_cursor, data_dict, sensor_id, message_id):
    
    GKN_RAIN_GAUGE_MM_PER_TICK = 0.254
    num_ticks = int(float(data_dict['Rain'])/GKN_RAIN_GAUGE_MM_PER_TICK)
    
    SQL_INSERT = """
        INSERT INTO rain_gauge_sensor_logs
            (num_ticks, period, log_datetime, rain_gauge_sensor_id,
             message_transaction_id)
        VALUES
            ({num_ticks}, 10, '{log_datetime}', {sensor_id},
             {message_id})
        ON DUPLICATE KEY UPDATE
            num_ticks = {num_ticks};
        
    """.format(num_ticks=num_ticks, sensor_id=sensor_id,
              log_datetime=data_dict['DTM'], message_id=message_id)
    
    db_cursor.execute(SQL_INSERT)

    update_message_parsed(db_cursor, message_id)

def parse_gkn_tilt_sensor_data(db_cursor, data_dict, sensor_id, message_id):
    
    param_name = list(data_dict.keys())[0]
    col_name = param_name.split("_",1)[1].lower()
    value = data_dict[param_name]
    
    SQL_INSERT = """
        INSERT INTO gkn_tilt_sensor_logs
            ({col_name}, log_datetime, gkn_tilt_sensor_id)
        VALUES
            ({value}, '{log_datetime}', {sensor_id})
        ON DUPLICATE KEY UPDATE
            {col_name} = {value};
        
    """.format(col_name=col_name, value=value, sensor_id=sensor_id,
              log_datetime=data_dict['DTM'])
    
    db_cursor.execute(SQL_INSERT)

    update_message_parsed(db_cursor, message_id)
    

def parse_data(db_cursor, msg_txn):
    data_dict = msg_txn.data_dict

    # # gateway rtc out of sync
    # data_dict['DTM'] = msg_txn.txn_datetime

    sensor_id = msg_txn.sensor_id
    message_id = msg_txn.message_id
    sensor_codes = data_dict.keys()
    
    if ('TIP' in sensor_codes) or ('INS' in sensor_codes):
        parse_rain_gauge_data(db_cursor, data_dict, sensor_id, message_id)

    elif 'BTV' in sensor_codes and 'VWC' in sensor_codes:
        parse_power_sensor_data(db_cursor, data_dict, sensor_id, message_id)
        parse_soil_moisture_data(db_cursor, data_dict, sensor_id, message_id)
    
    elif 'VWC' in sensor_codes or 'Soil' in list(sensor_codes)[0]:
        parse_soil_moisture_data(db_cursor, data_dict, sensor_id, message_id)

    elif 'BTV' in sensor_codes:
        parse_power_sensor_data(db_cursor, data_dict, sensor_id, message_id)

    elif 'AXL' in sensor_codes:
        parse_position_sensor_data(db_cursor, data_dict, sensor_id, message_id)
        
     # gkn_tilt_data
    elif 'Rain' in list(sensor_codes)[0]:
        parse_gkn_rain_sensor_data(db_cursor, data_dict, sensor_id, message_id)
        
    # gkn_tilt_data
    elif 'Tilt' in list(sensor_codes)[0]:
        parse_gkn_tilt_sensor_data(db_cursor, data_dict, sensor_id, message_id)

    else:
        raise Exception('No parser method for given Sensor Codes')


def is_old_sms_format(msg):
    float_pattern = "\d{1,2}\.\d{3,4}"
    pattern = "(?<=\$){fp},{fp},{fp}".format(fp=float_pattern)
    bits = re.findall(float_pattern, msg) 

    sms = re.findall("SMS", msg)

    if bits and sms:
        return bits
    else:
        return None

def parse_message(db_cursor, msg_txn):
    
    message = msg_txn.message
    log.info(message)

    try:
        info_field, data_field = tuple(message.split('$')[:2])
    except:
        raise ValueError("Error: Invalid message construction", message)
        
    bu_code, site_code, sensor_code = tuple(info_field.split("-")[:3])
    
    msg_txn.bu_code = bu_code
    msg_txn.site_code = site_code

    bits = is_old_sms_format(message)
    
    #sensor_code = get_gkn_sensor_code(sensor_code, data_field)
    
    if sensor_code == "GKN":
        try:
            sensor_code = re.search("^[A-Za-z]+\d{0,1}",data_field).group(0)
        except AttributeError:
            raise ValueError("Error: Wrong GKN data field construction", message)

        if sensor_code == "Soil":
            sensor_code = re.search("^[A-Za-z_0-9]+\d{0,1}",data_field).group(0)
            
    elif bits:
        # dtm part
        dtm = re.search("DTM\:\d+",message).group(0)
        data_field = "BTV:{a},BTA:{b},VWC:{c};{dtm}".format(a=bits[0],b=bits[1],c=bits[2],dtm=dtm)

    msg_txn.sensor_code = sensor_code
    
#     print(bu_code, site_code, sensor_code)
    
    logger_id, sensor_id = get_logger_sensor_id(db_cursor, bu_code, site_code, sensor_code)
    
    msg_txn.logger_id = logger_id
    msg_txn.sensor_id = sensor_id
    

    if logger_id is not None:
        msg_txn.data_dict = convert_data_dict(data_field)
        return msg_txn

    else:
        print("ERROR: No logger_id information", data_message)
        return None


def record_unparsed_message(db_cursor, id_message):
    SQL_UPDATE = '''
        UPDATE message_transactions
        SET
            error_parse_datetime = '2010-01-01'
        WHERE
            id = %s
    '''

    db_cursor.execute(SQL_UPDATE, (datetime.today(), id_message,))

def write_error_log(message_id, message, error_trace):
    print("ERROR:", message_id, message, error_trace, "\r\n")
    # with open("error.log", "a") as error_log:

    #     error_log.write(str(datetime.today()) + "\n")
    #     error_log.write(str(message_id) + "\n")
    #     error_log.write(message + "\n")

    #     error_log.write(error_trace + "\n")

def write_run_log(log_str):
    print(log_str)
    # with open("run.log", "a") as run_log:
    #     run_log.write(str(datetime.today()) + "\t")
    #     run_log.write(log_str + "\n")

def back_comp_msg(message):
    if re.search(",DTM", message):
        message = re.sub(r',DTM', ';DTM', message)
    
    if re.search("AXL\d\d", message):
        message = message.replace("AXL", "TLT")
        message = re.sub("\$(?!$)","$AXL:",message)
        message = re.sub(";",";MGR:0.0,0.0,0.0;",message, count=1)
        # print(message)
    elif re.search("MGR\d\d", message):    
        message = message.replace("MGR", "TLT")
        message = re.sub("\$(?!$)","$AXL:0.0,0.0,0.0;",message)
        message = re.sub(";",";MGR:",message, count=1)
        # print(message)
    
    return message

def get_arguments():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-l","--limit", help="max messages to process", type=int)

    args = parser.parse_args()

    if not args.limit:
        args.limit = 100

    return args

def setup_logger():
    log_formatter = logging.Formatter('%(asctime)s %(levelname)s %(funcName)s(%(lineno)d) %(message)s')

    logFile = 'parser_old.log'

    my_handler = RotatingFileHandler(logFile, mode='a', maxBytes=5*1024*1024, 
                                     backupCount=2, encoding=None, delay=0)
    my_handler.setFormatter(log_formatter)
    app_log = logging.getLogger('root')
    app_log.setLevel(logging.INFO)
    app_log.addHandler(my_handler)

    return app_log
        
log=setup_logger()

if __name__ == "__main__":
    import traceback
    import time

    args=get_arguments()

    # while True:
    log.info("Start Parsing")

    db_conn = connect_db()
    db_cursor = db_conn.cursor()

    messages = get_unparsed_messages(db_cursor, args.limit)
    # print("Messages:")
    # print(messages)

    log.info("Parsing N messages: " + str(len(messages)))

    for message_data in messages:
        message_id = message_data[0]
        message = message_data[1]
        # print(message_data)

        message = back_comp_msg(message)
        #log.info(str(message_id) + " " + message)

        try:

            msg_txn = MessageTransaction(message_id=message_id, message=message,
                txn_datetime=message_data[2])
            msg_txn = parse_message(db_cursor, msg_txn)
            parse_data(db_cursor, msg_txn)
            
        except Exception as e:
            # print("Error")
            record_unparsed_message(db_cursor, message_id)
            log.error(message_id, message, traceback.format_exc())

            # sys.exit()

        db_conn.commit()

    db_cursor.close()
    db_conn.close() 

    log.info("Parsed N messages: " + str(len(messages)))
    log.info("End Parsing")
    log.info(datetime.now().strftime("%c"))
    
    sys.exit()
        

