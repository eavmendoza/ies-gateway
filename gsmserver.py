from Adafruit_IO import MQTTClient
import time
from volmem import client as volmem_client
import sys
import mqtt.mqttlib as mqttlib
from dbio import txn
import datetime
from gsm import modem
import subprocess as sub
dt = datetime.datetime
td = datetime.timedelta

DBNAME = "edcrpidb"
TXNTABLE = "transactions"
DTFMT = "%Y-%m-%d %H-%M-%S"

gsm = modem.GsmModem('/dev/ttyGSM', 9600, 29, 22)

class NoMQTTConnectionException(Exception):
    pass

def get_messages(stat=0, limit=30, delay=None, recent=False):

    start_time_q = ""
    if delay:
        start_time = dt.today() - td(minutes=delay)
        start_time_q = "and dt > '{}'".format(start_time.strftime(DTFMT))

    query = ("select id, message as msg from {0}.{1} "
        "where stat = {2} {4} order by id desc limit {3}".format(DBNAME,
            TXNTABLE, stat, limit, start_time_q))

    return txn.read(query)

def update_messages_status(id_list=None, stat=0):
    if not id_list:
        raise ValueError("id_list must not be empty")

    ids_str = str(id_list)[1:-1]
    query = ("update {}.{} set stat = {} where id in ({})".format(DBNAME,
        TXNTABLE, stat, ids_str))
    # print(query)
    txn.write(query)


def log_to_remote(messages):
    msg_dict = {}
    for mid, msg in messages:
        msg_dict[mid] = msg


    published_ids = []

    for mid in msg_dict:
        print("Logging to remote:", msg_dict[mid], end='... ')
        stat = txn.sql_txn_log(msg_dict[mid], dbname="db_remote")
        if stat:
            published_ids.append(mid)
            print("Success!")
        else:
            print("ERROR: cannot log to remote")
        time.sleep(0.5)
    return published_ids

def sms_tx(messages):
    msg_dict = {}
    for mid, msg in messages:
        msg_dict[mid] = msg


    published_ids = []

    for mid in msg_dict:
        print("Sending through SMS:", msg_dict[mid], end='... ')
        beat("Y")
        stat = gsm.send_msg(msg_dict[mid], "09277837028")
        if stat==0:
            published_ids.append(mid)
            print("Success!")
            beat("G")
        else:
            print("ERROR: send through sms")
            beat("R")
        time.sleep(0.5)
    return published_ids

def beat(color='R'):
    sub.Popen(['sudo','python3.6', '/home/pi/gateway2/sensors/neo.py', '-f'+color, 
        '-d10'], stdout=sub.PIPE, stderr=sub.STDOUT)

def server():
    print("Setting up memory ...", end='')
    mc_client = volmem_client.get()
    df_pub_list = mc_client.get("df_pub_list")
    gsm.set_defaults()

    while True:

        recent_messages = get_messages(limit=10, delay=10)
        old_messages = get_messages(limit=50)
        if recent_messages:
            print("Sending recent messages")
            # published_ids = log_to_remote(recent_messages)
            published_ids = sms_tx(recent_messages)
            update_messages_status(id_list=published_ids, stat=1)
        elif old_messages:
            print("Sending old messages")
            # published_ids = log_to_remote(old_messages)
            published_ids = sms_tx(old_messages)
            update_messages_status(id_list=published_ids, stat=1)
        else:
            print("No more  messages to log")
            time.sleep(10)

if __name__ == "__main__":

    while True:
        try:
            server()
        except KeyboardInterrupt:
            print("Bye")
            sys.exit()
