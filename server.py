from Adafruit_IO import MQTTClient
import time
from volmem import client as volmem_client
import sys
import mqtt.mqttlib as mqttlib
from dbio import txn
import datetime
dt = datetime.datetime
td = datetime.timedelta

DBNAME = "edcrpidb"
TXNTABLE = "transactions"
DTFMT = "%Y-%m-%d %H-%M-%S"

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


def publish_pub_list(mqtt_client, mc_client):
    unpublished_list = mc_client.get("pub_list")
    # print(unpublished_list)

    # print("Server running")
    mqtt_feed_name = mc_client.get("gateway_config")["mqtt"]["feed"]

    res, stat = None, None

    # for index, row in unpublished_list.iterrows():
        # msg_str = row['msg']
    for msg_str in unpublished_list:
        print(msg_str)

        try:
            if mqtt_client.is_connected():
            # if mqtt_client:
                res, stat = mqtt_client.publish(mqtt_feed_name, msg_str)
                pub_stat = 1
            else:
                print("Not connected")
                pub_stat = 0
        except AttributeError:
            print("MQTT client does not exist")
            pub_stat = 0

        print(res, stat)

        print("Save to memory ", end="")
        dbtxn.sql_txn_log(msg_str, pub_stat)
        print("done")

        # df_pub_list.loc[index, 'stat'] = 1

    # print(df_pub_list)

    # get smsoutbox for new messages inserted while sending
    df_pub_list_updated = mc_client.get("df_pub_list")

    # get all items later than the latest ts of smsoutbox
    ts_latest = df_pub_list.ts.max()
    df_pub_list_new_inserts = df_pub_list_updated[df_pub_list_updated.ts > ts_latest]

    # append new items in existing smsoutbox
    df_pub_list = df_pub_list.append(df_pub_list_new_inserts,
        ignore_index = True, sort=True)

    mc_client.set("df_pub_list", df_pub_list)

def publish_messages(messages, mqtt_client, mqtt_feed_name):
    msg_dict = {}
    for mid, msg in messages:
        msg_dict[mid] = msg

    published_ids = []

    for mid in msg_dict:
        print("Publishing:", msg_dict[mid], end='... ')
        mqtt_client.publish(mqtt_feed_name, msg_dict[mid])
        print("done")
        published_ids.append(mid)
        time.sleep(1)

    return published_ids

def log_to_remote(messages):
    msg_dict = {}
    for mid, msg in messages:
        msg_dict[mid] = msg


    published_ids = []
    unpublished_ids = []

    for mid in msg_dict:
        print("Logging to remote:", msg_dict[mid], end='... ')
        stat = txn.sql_txn_log(msg_dict[mid], dbname="db_remote")
        if stat:
            published_ids.append(mid)
            print("Success!")
        else:
            print("ERROR: cannot log to remote")
            unpublished_ids.append(mid)
        time.sleep(0.5)
    return published_ids, unpublished_ids

def server():
    print("Setting up memory ...", end='')
    mc_client = volmem_client.get()
    df_pub_list = mc_client.get("df_pub_list")

    while True:

        recent_messages = get_messages(limit=10, delay=10)
        old_messages = get_messages(limit=50)
        if recent_messages:
            print("Sending recent messages")
            published_ids, unpublished_ids = log_to_remote(recent_messages)
            if published_ids:
                update_messages_status(id_list=published_ids, stat=1)
            if unpublished_ids:
                update_messages_status(id_list=unpublished_ids, stat=-1)
            
        elif old_messages:
            print("Sending old messages")
            published_ids, unpublished_ids = log_to_remote(old_messages)
            if published_ids:
                update_messages_status(id_list=published_ids, stat=1)
            if unpublished_ids:
                update_messages_status(id_list=unpublished_ids, stat=-1)
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
