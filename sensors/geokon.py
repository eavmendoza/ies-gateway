import serial
from volmem import client
from datetime import datetime as dt
from dbio import txn
import time
import sys
import re
import volmem.client
import argparse
import subprocess as sub

CRLN = bytes("\r","utf-8")
GDCMD = bytes("7\r","utf-8")

def get_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-p","--publish", help="publish data",
        action="store_true")
    parser.add_argument("-m","--matches", help="print matches",
        action="store_true")
    # parser.add_argument("-d","--delay", help="delay", type=int)

    args = parser.parse_args()
        
    if args.publish:
        print("Will publish data")
    else:
        print("Messages NOT published")

    return args

def get_prompt(ser):
    prompt_retry = 0
    while True:
        beat("Y")
        print("Waiting for prompt..")
        ser.write(CRLN)
        retry = 0
        prompt = ""
        while True:
            prompt += ser.read(ser.in_waiting).decode("utf-8")
            if ("CR1000>" in prompt):
                print(prompt)
                return
            else:
                time.sleep(0.2)
                retry += 1
                if retry == 5:
                    break

        prompt_retry += 1
        if prompt_retry == 10:
            print("ERROR: Could not connect to Geokon logger")
            sys.exit()

def get_until_no_in_waiting(ser):
    block = ""
    while ser.in_waiting > 0:
        block += str(ser.read(ser.in_waiting).decode("utf-8"))
    return block

def beat(color='R'):
    sub.Popen(['sudo','python3.6', '/home/pi/gateway2/sensors/neo.py', '-f'+color, '-d10'], stdout=sub.PIPE, stderr=sub.STDOUT)

def get_data(ser):
    lines = ""
    ser.write(GDCMD)
    time.sleep(0.5)
    
    while True:
        beat("W")
        # print("Getting data")
        data = ""
        while True:
            # data = get_until_no_in_waiting(ser)
            ser.flush()
            data = str(ser.readline().decode("utf-8"))
            lines+=data
            if ("more" in data):
                print(data)
                time.sleep(1)
                ser.write(CRLN)
                break
            elif ("Alarm" in data):
                ser.write(CRLN)
                # print(data)
                # get_prompt(ser)
                return lines

    #     txn.sql_txn_log(message_value)

if __name__ == "__main__":

    args = get_arguments()
    try:
        ser = serial.Serial(port='/dev/ttyUSB0', baudrate=9600, timeout=2)  # open serial port
    except serial.serialutil.SerialException:
        print("ERROR: Geokon logger may not be connected")
        sys.exit()

    for i in range(1,4):
        print(".", end="")
        ser.write(CRLN)
        time.sleep(1)

    get_prompt(ser)

    try:
        geokon_data = get_data(ser)
        # print("Lines:", geokon_data)
    except KeyboardInterrupt:
        print("Bye")
        ser.close()

    # format data
    matches = re.findall("[A-Za-z_0-9]+: [0-9\.\-]+", geokon_data)
    if args.matches:
        print(matches)

    coded_dt = dt.today().strftime("%y%m%d%H%M%S")

    mc = volmem.client.get()
    cnf = mc.get("gateway_config")
    g_name = cnf["gateway"]["name"]
    print(g_name)

    for item in matches:
        message_value = "{}-GKN${};DTM:{}$".format(g_name,item,coded_dt)
        print(message_value)
        if args.publish:
            txn.sql_txn_log(message_value)
        

