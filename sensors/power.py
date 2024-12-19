import logging
from ina219 import INA219
import time
import argparse
import sys
from datetime import datetime as dt
# from volmem import client
# from dbio import txn

SHUNT_OHMS = 0.1
MAX_EXPECTED_AMPS = 0.5
ADDRESS = client.get().get("gateway_config")["ina219"]["address"]

def read(print_val=False):
    ina = INA219(SHUNT_OHMS, MAX_EXPECTED_AMPS, address=ADDRESS)
    ina.configure(ina.RANGE_16V, ina.GAIN_AUTO)

    if print_val:
        print("Bus Voltage    : %.3f V" % ina.voltage())
        print("Bus Current    : %.3f mA" % ina.current())
        print("Supply Voltage : %.3f V" % ina.supply_voltage())
        print("Shunt voltage  : %.3f mV" % ina.shunt_voltage())
        print("Power          : %.3f mW" % ina.power())

    return ina

def get_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-p","--publish", help="publish data",
        action="store_true")
    parser.add_argument("-d","--delay", help="delay", type=int)
    parser.add_argument("-t","--test", help="test", action="store_true")

    args = parser.parse_args()
        
    if args.publish:
        print("Will publish data")

    if not args.delay:
        args.delay = 30
    print("Delay={}".format(args.delay))
    
    return args

def read_publish(delay):
    ina = read()

    btv = round(ina.supply_voltage(),2)
    cur = round(ina.current(),2)
    ts = dt.today().strftime("%y%m%d%H%M%S")

    mc = client.get()
    cnf = mc.get("gateway_config")
    gateway_name = "{}-{}".format(cnf["gateway"]["name"], 
        cnf["gateway"]["logger"])

    message_value = "{}$BTV:{};BTA:{};DTM:{}$".format(gateway_name,
        btv, cur, ts)

    print(message_value)

    # client.push_pub_list(message_value)
    # time.sleep(delay)
    # client.push_df_pub_list(message_value)
    txn.sql_txn_log(message_value)


if __name__ == "__main__":

    args = get_arguments()

    if args.publish:
        read_publish(args.delay)
        sys.exit()

    if args.test:
        try:
            while True:
                read(print_val=True)
                time.sleep(5)
        except KeyboardInterrupt:
            print("Bye")

    read(print_val=True)
    