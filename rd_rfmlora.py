"""
Wiring Check, Pi Radio w/RFM9x
 
Learn Guide: https://learn.adafruit.com/lora-and-lorawan-for-raspberry-pi
Author: Brent Rubell for Adafruit Industries
"""
import time
import busio
from digitalio import DigitalInOut, Direction, Pull
import board
import adafruit_rfm9x
from datetime import datetime as dt
from datetime import timedelta as td
from dbio import txn
import sys
import subprocess as sub
import re
import queue
import threading
import json
from config import config

# logfile = "/home/pi/lora.logs"
# logging.basicConfig(filename=logfile, level=logging.INFO,
#     format='%(asctime)s - %(message)s', filemode='a',
#     datefmt='%Y-%m-%d %H:%M:%S',
#     )

def blink():
    sub.Popen(['python3', '/home/pi/gateway2/sensors/led.py', '-d100'], stdout=sub.PIPE, stderr=sub.STDOUT)


def process_message(rfm9x, packet, site_code):
# def process_message(rfm9x, prev_packet, site_code):
    try:
        packet_text = packet[4:].decode("utf-8", "replace")
        packet_text = re.sub("[^\w\-\:\$\,\.\!\;]","",packet_text)
        if len(packet_text)<10:
            print("Packet error: len too short. Ignoring message")
            return
    except:
        print("Cannot recover message")
        return

    # if "ACKAXL" in packet_text or "ACKSMS" in packet_text:
    # if "ACK" in packet_text:
    #     print("Received:", packet_text, "Ignoring ...")
    #     return

    ts = dt.today().strftime("%y%m%d%H%M%S")
    message_value = "{},DTM:{}$".format(packet_text,ts)
    message_value = re.sub("\!B\!","",message_value)
    # print(packet_text)
    
    # if "GEN" in message_value:
    #     message_value = message_value.replace("GEN",site_code)
        # print("Msg to store:", message_value)
        # txn.sql_txn_log(msg=message_value, table="transactions")
    # elif "INS" in message_value:
        # print(message_value)
        # print("Msg to store:", message_value)
        # txn.sql_txn_log(msg=message_value, table="instantaneous_rain")
    # else:
        # print("Msg to store:", message_value)
        # txn.sql_txn_log(msg=message_value, table="transactions")

    # try:
    #     sense_id = re.search("(?<=[A-Z])\d+(?=\$)",message_value).group(0)
    # except AttributeError:
    #     print("ERROR: Cannot find sense_id")
    #     return

    # try:
    #     msg_type = re.search("(?<=-)[A-Z]{3}(?=\d)", message_value).group(0)
    # except AttributeError:
    #     msg_type = "ACK"

    out = {}
    out['id'] = str(packet[1])
    out['rssi'] = str(rfm9x.rssi)
    out['msg'] = message_value

    print(json.dumps(out))

    # logline = "[ID]: "+str(packet[1])+" "
    # logline += "[RSSI]: "+str(rfm9x.rssi)+" "
    # logline += "[MSG]: "+message_value+" "


def setup_radio(cnf):
    CS = DigitalInOut(board.CE1)
    RESET = DigitalInOut(board.D25)
    spi = busio.SPI(board.SCK, MOSI=board.MOSI, MISO=board.MISO)

    FREQ = float(cnf["this_gateway"]["gateway"]["freq"])
    ID = cnf["this_gateway"]["gateway"]["radio_id"]

    # print(sys.argv[1])

    out={}

    try:
        # mur 867
        # smr 869
        # rfm9x = adafruit_rfm9x.RFM9x(spi, CS, RESET, 433.0)
        rfm9x = adafruit_rfm9x.RFM9x(spi, CS, RESET, FREQ)
        rfm9x.tx_power = 20

        rfm9x.enable_crc = True
        # set delay before sending ACK
        rfm9x.ack_delay = 0.15
        # set node addresses
        rfm9x.node = ID
        rfm9x.receive_timeout = 30.0
        # rfm9x.signal_bandwidth = 125000
        # rfm9x.spreading_factor = 11
        # rfm9x.coding_rate = 5
        # rfm9x.receive_timeout = 30.0
        # print('RFM9x: Detected')
        # print('Addr:', rfm9x.node)
        # print("FREQ:", rfm9x.frequency_mhz)
        # print("BW:",rfm9x.signal_bandwidth)
        # print("CR:", rfm9x.coding_rate)
        # print("SF:", rfm9x.spreading_factor)
        # print("Receive timeout", rfm9x.receive_timeout)


        out["rfm9x"] = "Detected"
    except RuntimeError:
        # Thrown on version mismatch
        # print('RFM9x: ERROR')
        out["rfm9x"] = "ERROR"
        print(json.dumps(out))
        sys.exit()


    out["addr" ] = rfm9x.node
    out["freq"] = rfm9x.frequency_mhz
    out["bw"] = rfm9x.signal_bandwidth
    out["cor"] = rfm9x.coding_rate
    out["sf"] = rfm9x.spreading_factor
    out["crc"] = rfm9x.enable_crc
    out["time"] = rfm9x.receive_timeout

    print(json.dumps(out))


    return rfm9x

def main():
    # Configure RFM9x LoRa Radio
    # mc = client.get()
    cnf = config.get_yaml()

    
    site_code = cnf["this_gateway"]["gateway"]["station_code"]
    SITECODE = site_code[:3]
    counter = 0

    rfm9x = setup_radio(cnf)
    this_txbuffer = ""
    successive_send = 0

    while True:

        packet = rfm9x.receive(with_ack=True, keep_listening=True, with_header=True)

        if packet is not None:
            # print("\n#########################")
            # print("RSSI:", rfm9x.rssi)
            # prev_packet = packet[4:].decode("utf-8", "replace")
            # rfm9x.destination = packet[1]
            # print("From:", packet[1])

            process_message(rfm9x, packet, SITECODE)


    print("Done")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Closing lora!")

# update 11/23/2020 1234

