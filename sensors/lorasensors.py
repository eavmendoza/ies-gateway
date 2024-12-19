import serial
from volmem import client
from datetime import datetime as dt
from dbio import txn

def relay_serial_messages():
    ser = serial.Serial(port='/dev/ttyAMA0', baudrate=9600)  # open serial port

    while True:
        try:
            lineRaw = ser.readline().decode("utf-8")
            #print("RAW Serial Feed::::")
            #print(lineRaw)
            #line = lineRaw.strip().decode("utf-8")
            line = lineRaw.replace("\r"," ").strip()
        except UnicodeDecodeError:
            print(">> Caught UnicodeDecodeError. Skipping line")
            continue

        ts = dt.today().strftime("%y%m%d%H%M%S")
        message_value = "{};DTM:{}$".format(line[:-1],ts).replace(" ","") 

        print(message_value)

        # client.push_df_pub_list(message_value)
        txn.sql_txn_log(message_value)

if __name__ == "__main__":

    try:
        relay_serial_messages()
    except KeyboardInterrupt:
        print("Bye")
