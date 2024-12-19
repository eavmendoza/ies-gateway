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
import volmem.client as client
from datetime import datetime as dt
from datetime import timedelta as td
from dbio import txn
import sys
import subprocess as sub
import re
import queue
import threading
import logging
from volmem import client as volmem_client

def blink():
    sub.Popen(['python3', '/home/pi/gateway2/sensors/led.py', '-d100'], stdout=sub.PIPE, stderr=sub.STDOUT)


def process_message(rfm9x, prev_packet, site_code, this_txbuffer):
# def process_message(rfm9x, prev_packet, site_code):
    try:
        packet_text = prev_packet
        packet_text = re.sub("[^\w\-\:\$\,\.\!]","",packet_text)
        print("Packet_text:", packet_text)
        if len(packet_text)<20:
            print("Packet error: len too short. Ignoring message")
            return
    except:
        print("Cannot recover message")
        return

    # if "ACKAXL" in packet_text or "ACKSMS" in packet_text:
    if "ACK" in packet_text:
        print("Received:", packet_text, "Ignoring ...")
        return

    ts = dt.today().strftime("%y%m%d%H%M%S")
    message_value = "{};DTM:{}$".format(packet_text,ts)
    message_value = re.sub("\!B\!","",message_value)
    # print(packet_text)
    
    if "GEN" in message_value:
        message_value = message_value.replace("GEN",site_code)
        print("Msg to store:", message_value)
        txn.sql_txn_log(msg=message_value, table="transactions")
    elif "INS" in message_value:
        # print(message_value)
        print("Msg to store:", message_value)
        txn.sql_txn_log(msg=message_value, table="instantaneous_rain")
    else:
        print("Msg to store:", message_value)
        txn.sql_txn_log(msg=message_value, table="transactions")

    try:
        sense_id = re.search("(?<=[A-Z])\d+(?=\$)",message_value).group(0)
    except AttributeError:
        print("ERROR: Cannot find sense_id")
        return

    try:
        msg_type = re.search("(?<=-)[A-Z]{3}(?=\d)", message_value).group(0)
    except AttributeError:
        msg_type = "ACK"

    # if (int(sense_id) > 59 or msg_type == "RG") or (int(sense_id) in [45,46]):
    #     tx_msg = "ACK{msg_type}{sid}".format(msg_type=msg_type,sid=sense_id)
    # else:
    #     tx_msg = "ACK".format(msg_type=msg_type,sid=sense_id)

    # tx_msg = "ACK{msg_type}{sid}".format(msg_type=msg_type,sid=sense_id)

    # txm_new = {'tts': dt.today()+td(seconds=1),'message': tx_msg}

    # txbuffer = this_txbuffer.get()

    # if len(txbuffer)>0:
    #     txm = txbuffer.pop(0)
    #     if len(txm['message']) < 70:
    #         txm['message'] = txm['message'] + tx_msg
    #         txm['tts'] = dt.today()
    #         txbuffer.insert(0,txm)
    #     else:
    #         txbuffer.insert(0,txm)
    #         txbuffer.insert(0,txm_new)
    # else:
    #     txbuffer.insert(0,txm_new)

    # this_txbuffer.set(txbuffer)

class TxBuffer:
    def __init__(self):
        mc_client = volmem_client.get()
        mc_client.set('txbuffer',list())

    def set(self,txbuffer):
        mc_client = volmem_client.get()
        mc_client.set('txbuffer',txbuffer)

    def get(self):
        mc_client = volmem_client.get()
        return mc_client.get('txbuffer')

def setup_radio(cnf):
    CS = DigitalInOut(board.CE1)
    RESET = DigitalInOut(board.D25)
    spi = busio.SPI(board.SCK, MOSI=board.MOSI, MISO=board.MISO)

    FREQ = float(cnf["gateway"]["lorafreq"])

    try:
        # mur 867
        # smr 869
        #rfm9x = adafruit_rfm9x.RFM9x(spi, CS, RESET, 868.0)
        rfm9x = adafruit_rfm9x.RFM9x(spi, CS, RESET, FREQ)
        rfm9x.tx_power = 23
        
        # rfm9x.signal_bandwidth = 31250
        # rfm9x.spreading_factor = 9
        # rfm9x.coding_rate = 8

        # rfm9x.signal_bandwidth = 62500
        # rfm9x.spreading_factor = 9
        # rfm9x.coding_rate = 8

        rfm9x.enable_crc = True
        # set delay before sending ACK
        rfm9x.ack_delay = 0.1
        # set node addresses
        rfm9x.node = 0
        rfm9x.receive_timeout = 30.0
        # rf95.setModemConfig(Bw31_25Cr48Sf512)
        print('RFM9x: Detected')
        print("FREQ:", FREQ)
        print("BW:",rfm9x.signal_bandwidth)
        print("CR:", rfm9x.coding_rate)
        print("SF: ", rfm9x.spreading_factor)
        print("Receive timeout", rfm9x.receive_timeout)

    except RuntimeError:
        # Thrown on version mismatch
        print('RFM9x: ERROR')
        sys.exit()


    return rfm9x

def main():
    # Configure RFM9x LoRa Radio
    mc = client.get()
    cnf = mc.get("gateway_config")
    site_code = cnf["gateway"]["name"]
    SITECODE = site_code[:3]
    counter = 0

    rfm9x = setup_radio(cnf)
    this_txbuffer = TxBuffer()
    successive_send = 0

    logger = logging.getLogger(__name__)
    handler = logging.FileHandler('lora.logs')
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    while True:
        # print("Sending")
        # reply_bytes = bytes("test","utf-8")
        # success = rfm9x.send(data=reply_bytes)
        # print(success)
        # time.sleep(2)
        # continue


        packet = rfm9x.receive(with_ack=True, keep_listening=True, with_header=True)

        if packet is not None:
            print("\n#########################")
            print("RSSI:", rfm9x.rssi)
            prev_packet = packet[4:].decode("utf-8", "replace")
            rfm9x.destination = packet[1]
            rfm9x.send_with_ack(bytes("ACK","UTF-8"))
            # if len(prev_packet) < 20:
            #     print("Skipping message processing. prev_packet maybe empty")
            #     packet = None
            #     continue

            # try:
            #     packet_text = str(prev_packet, "utf-8")
            ledthread = threading.Thread(target=blink, daemon=True)
            ledthread.start()
            msgthread = threading.Thread(target=process_message, 
                # args=(rfm9x, prev_packet, SITECODE, ),
                args=(rfm9x, prev_packet, SITECODE, this_txbuffer),
                # args=(rfm9x, prev_packet, SITECODE),
                daemon=True)
            msgthread.start()

            # keep_listening=True,
            # destination=packet[0]
        

            # txbuffer = msgthread.join()

            # successive_send = 0

        # txbuffer = this_txbuffer.get()
        
        # if len(txbuffer)>0:
        #     # txbuffer_temp = txbuffer
        #     txm = txbuffer.pop(0)
        #     if dt.today() <= txm['tts']:
        #         # print("Send for later")
        #         txbuffer = txbuffer.insert(0, txm)
        #         continue

        #     print("\n********************************")
        #     print("Sending", txm['message'])
        #     # if dt.today() > txm['tts']:
        #     reply_bytes = bytes(txm['message'],"utf-8")

        #     send_stat = rfm9x.send(data=reply_bytes, keep_listening=True)
        #     # print(send_stat)
        #     # rfm9x.send(data=reply_bytes)

        #     successive_send += 1

        #     if successive_send > 20:
        #         logger.warn("successive_send reached max count (20)")
        #         txbuffer = list()
        #         successive_send = 0

        #     this_txbuffer.set(txbuffer)


    print("Done")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Closing lora!")

# update 11/23/2020 1234

