from Adafruit_IO import MQTTClient
import Adafruit_IO.errors as io_errors
import time
import sys
import mqtt.mqttlib as mqttlib
from parser import parser
import threading
import json
import yaml
import numpy
import math

creds = yaml.load(open('/home/ies/gateway/config/mqtt.yaml'))

def setup_logger():
    from logging.handlers import RotatingFileHandler
    import logging

    log_formatter = logging.Formatter('%(asctime)s %(levelname)s %(funcName)s(%(lineno)d) %(message)s')

    logFile = 'mqtt_server.log'

    my_handler = RotatingFileHandler(logFile, mode='a', maxBytes=5*1024*1024, 
                                     backupCount=2, encoding=None, delay=0)
    my_handler.setFormatter(log_formatter)
    app_log = logging.getLogger('root')
    app_log.setLevel(logging.INFO)
    app_log.addHandler(my_handler)

    return app_log

def disconnected(client):
   # Disconnected function will be called when the client disconnects.
   print('Disconnected from Adafruit IO!')
   raise DisconnectException

def random_delay():
    import random
    import time
    time.sleep(1+random.randint(0, 240))

def connected(client):
    # Connected function will be called when the client is connected to Adafruit IO.
    # This is a good place to subscribe to feed changes.  The client parameter
    # passed to this function is the Adafruit IO MQTT client so you can make
    # calls against it easily.
    # print("Connected to Adafruit IO!  Listening for {0} "
    # "changes...".format(THROTTLE_FEED_ID))
    # Subscribe to changes on a feed named DemoFeed.
    client.subscribe(creds["adafruit"]["feed_id"])
    # print("Connected to Adafruit IO! ")

def spawn_parse_data(msg=None):
    from subprocess import Popen, PIPE

    process = Popen(['/home/ies/pyenv/env/bin/python', '/home/ies/gateway/data_parser.py', '-l100'], stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()


def publish_delayed(feed, value):
    random_delay()

    mqttlib.publish(
        feed=feed,
        value=value,
        username=creds["adafruit"]["username"], 
        key=creds["adafruit"]["key"]
    )

def publish(feed, value):
    publish_thread = threading.Thread(target=publish_delayed, 
        args=(feed, value),
        daemon=True)    
    publish_thread.start()

    # mqttlib.publish(
    #     feed=feed,
    #     value=value,
    #     username=creds["adafruit"]["username"], 
    #     key=creds["adafruit"]["key"]
    # )    

def process_cummulative_rainfall(s_id):
    from analysis import rainfall
    from dbio.connect import connsql

    conn = connsql()

    for i in [1,2,24,72]:
        try:
            mov_sum = rainfall.get_moving_rainfall_sum(s_id,i,
                conn["logs"]).values[0][0]
        except TypeError:
            print("ERROR getting moving sum")
            log.error(f"ERROR: Moving sum {s_id}")
            continue

        try:
            publish(f"rain_{str(s_id)}_{str(i)}h", mov_sum)
        except:
            log.error(f"ERROR: publishing rain {s_id}")

    # if (sum_1h): 
    #     publish(f"rain_{s_id}_1h", sum_1h)
    
    # sum_2h = get_moving_rainfall_sum(s_id,2).values[0][0]:
    #     publish(f"rain_{s_id}_2h", sum_2h)
    # sum_24h = get_moving_rainfall_sum(s_id,24).values[0][0]:
    #     publish(f"rain_{s_id}_1h", sum_1h)
    # sum_72h = get_moving_rainfall_sum(s_id,72).values[0][0]:
    #     publish(f"rain_{s_id}_1h", sum_1h)


def process_feed(payload):

    insert_thread = threading.Thread(target=insert_msg, 
        args=(payload,),
        daemon=True)    
    insert_thread.start()

    parser_thread = threading.Thread(target=spawn_parse_data, 
        args=(payload,),
        daemon=True)    
    parser_thread.start()

    # insert_msg(payload)

    payload = parser.back_comp_msg(payload)
    msg_dict = parser.parse_message(payload)

    # print(msg_dict)
    log.info(str(msg_dict))

    if msg_dict["msg_type"]=="rain":
        s_id = msg_dict["rain_props"]["sensor_id"]
        feed_name = "rain_"+str(s_id)
        
        publish(
            feed=feed_name,
            value=float(msg_dict["msg"]["TI"])*0.254,
        )

        process_thread = threading.Thread(target=process_cummulative_rainfall, 
            args=(s_id,),
            daemon=True)    
        process_thread.start()


    if "soil_props" in msg_dict.keys():
        publish(
            feed="soil_" + str(msg_dict["soil_props"]["sensor_id"]),
            value=float(msg_dict["msg"]["SO"]) * 100.0,
        )

    if "tilt_props" in msg_dict.keys():
        publish(
            feed="tilt_" + str(msg_dict["tilt_props"]["sensor_id"]) + "_x",
            value=numpy.arcsin(float(msg_dict["msg"]["AX"]))*180/math.pi,
        )

        publish(
            feed="tilt_" + str(msg_dict["tilt_props"]["sensor_id"]) + "_y",
            value=numpy.arcsin(float(msg_dict["msg"]["AY"]))*180/math.pi,
        )

        publish(
            feed="tilt_" + str(msg_dict["tilt_props"]["sensor_id"]) + "_z",
            value=numpy.arcsin(float(msg_dict["msg"]["AZ"]))*180/math.pi,
        )

    if "power_props" in msg_dict.keys():
        try:
            # gateway power log
            publish(
                feed="gtw_volt_" + str(msg_dict["msg"]["PI"]),
                value=float(msg_dict["msg"]["BV"]),
            )
            
            # publish(
            #     feed="gtw_pwr_" + str(msg_dict["msg"]["PI"]),
            #     value=float(msg_dict["msg"]["PW"]),
            # )
            
        except KeyError:
            # sensor power log
            pass
            # publish(
            #     feed="sensor_volt_" + str(msg_dict["power_props"]["code_name"]),
            #     value=float(msg_dict["msg"]["BV"]),
            # )

def message(client, feed_id, payload):
    # Message function will be called when a subscribed feed has a new value.
    # The feed_id parameter identifies the feed, and the payload parameter has
    # the new value.
    # raise ThrottleException(payload)

    print('Feed {0} received new value: {1}'.format(feed_id, payload))

    payload = str(payload)

    try:
        payload_dict = json.loads(payload)
        payload = payload_dict["msg"]
    except json.decoder.JSONDecodeError:
        print(payload)

    # if type(payload)==dict:
    #     payload=payload

    process_thread = threading.Thread(target=process_feed, 
        args=(payload,),
        daemon=True)    
    process_thread.start()

    
    # parse_feed(payload)

log=setup_logger()


def insert_msg(msg):
    from dbio import txn,connect

    print(f"Insert to db {msg}", end=" ... ")
    conn = connect.connsql()
    stat = txn.txn_log(msg, conn["logs"], "mqttlog")
    # print(stat)
    # if not stat:
    print("done")
    # else:
    #     print("ERROR")
    # time.sleep(0.5)



def server():
    
    import yaml
    creds = yaml.load(open('/home/ies/gateway/config/mqtt.yaml'))

    mqtt_client = MQTTClient(creds["adafruit"]["username"], creds["adafruit"]["key"])
    mqtt_client.on_disconnect = disconnected
    mqtt_client.on_message = message
    mqtt_client.on_connect = connected

    mqtt_client.connect()

    while True:
    # Poll the message queue
        try:
            mqtt_client.loop_background()
            
        except (ValueError, RuntimeError, io_errors.MQTTError) as e:
            print("Failed to get data, retrying\n", e)
            client.connect()
            continue

if __name__ == "__main__":

    # import data_parser
    server()

    # # insert_msg("test")
