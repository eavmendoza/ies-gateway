import serial
from datetime import datetime as dt
import time
import sys
import re
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


    import yaml
    from pathlib import Path
    config_dir = str(Path(__file__).parent.absolute()) + "/spyder/"

    with open(str(Path(__file__).parent.absolute())+"/setup/this_gateway.yaml") as stream:
        try:
            this_gateway = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)

    station_code = this_gateway["this_gateway"]["gateway"]["station_code"]

    import json

    props = {}
    with open(config_dir+'gkn_tilt.json') as json_file:
        props["tilt"] = json.load(json_file)[station_code]

    with open(config_dir+'gkn_soil.json') as json_file:
        props["soil"] = json.load(json_file)[station_code]

    with open(config_dir+'gkn_rain.json') as json_file:
        props["rain"] = json.load(json_file)[station_code]

    with open(config_dir+'gkn_batt.json') as json_file:
        props["batt"] = json.load(json_file)[station_code]

    print(props)
    

    from dbio import txn,connect

    msgs = []
    tilt_msgs = {}
    for name in props['tilt'].keys():
        tilt_msgs[name]= f"NI:{props['tilt'][name]},DT:{coded_dt},"

    print(tilt_msgs)

    for item in matches:
        # msg = "{}-GKN${},DT:{}".format(g_name,item,coded_dt)
        try:
            if "DL_Batt" in item:
                volt = re.search("(?<=Batt: )\d{1,3}\.\d{1,6}",item).group(0)
                msg = f"PI:{props['batt']['DL_Batt']},BV:{volt},DT:{coded_dt}"
                msgs.append(msg)
            elif "Rain" in item:
                val = re.search("(?<=Rain: )\d{1,3}\.{0,1}\d{0,6}",item).group(0)
                msg = f"RI:{props['rain']['Rain']},TI:{val},DT:{coded_dt}"
                msgs.append(msg)
            elif "Soil" in item:
                val = re.search("(?<=Soil_Moisture_\d: )-{0,1}\d{1,3}\.{0,1}\d{0,6}",item).group(0)
                s_id = re.search("Soil_Moisture_\d",item).group(0)
                msg = f"MI:{props['soil'][s_id]},SO:{val},DT:{coded_dt}"
                msgs.append(msg)
            elif "Angle" in item:
                val = re.search("(?<=Tilt\d_Angle_[AB]: )-{0,1}\d{1,3}\.\d{0,10}",item).group(0)
                s_id = re.search("Tilt\d",item).group(0)
                axis = re.search("(?<=Angle_)[AB]", item).group(0)
                tilt_msgs[s_id] += f"A{axis}:{val},"
            elif "Volts" in item:
                val = re.search("(?<=Tilt\d_Volts_[AB]: )-{0,1}\d{1,3}\.\d{0,10}",item).group(0)
                s_id = re.search("Tilt\d",item).group(0)
                axis = re.search("(?<=Volts_)[AB]", item).group(0)
                tilt_msgs[s_id] += f"V{axis}:{val},"
            elif "Temp" in item:
                val = re.search("(?<=Tilt\d_Temp: )\d{1,3}\.\d{0,10}",item).group(0)
                s_id = re.search("Tilt\d",item).group(0)
                tilt_msgs[s_id] += f"TP:{val},"
            else:
                print(f"Unknown msg: {item}")
                continue
        except (AttributeError):
            print(f"Conversion error: {item}")
            continue

        # msgs.append(msg)

    for key in tilt_msgs.keys():
        msgs.append(tilt_msgs[key][:-1])

    for msg in msgs:
        print(msg)

    if args.publish:
        for msg in msgs:
            # txn.sql_txn_log(message_value)

            print(f"Insert to db {msg}", end=" ... ")
            conn = connect.connsql()
            stat = txn.txn_log(msg, conn["logs"], "geokonlog")
            # print(stat)
            # if not stat:
            print("done")
        

