import serial
from volmem import client
from datetime import datetime as dt
from dbio import txn
import itertools
import time

def relay_serial_messages():
    # ser = serial.Serial('/dev/ttyUSB0', 115200, timeout=1, rtscts=True)  # open serial port
    ser = serial.Serial('/dev/ttyUSB0', 115200, timeout=1)  # open serial port
    CAR_RETURN = '\r'.encode('utf-8')
    SAMPLE = '7\r'.encode('utf-8')

    for _ in itertools.repeat(None, 10):
        ser.write(CAR_RETURN)
        lineRaw = ser.read_until('>')
        if len(lineRaw.decode('utf-8')) > 0:
            print("Response", lineRaw)
            break
        time.sleep(0.5)

    ser.write(SAMPLE)

    # for _ in itertools.repeat(None, 10):
    #     ser.write(SAMPLE)
    #     lineRaw = ser.read_until(':')
    #     if len(lineRaw.decode('utf-8')) > 0:
    #         print("Response", lineRaw)
    #         break
    #     time.sleep(0.5)
    # print(lineRaw)

    # ser.write('7'.encode('utf-8'))
    # for _ in itertools.repeat(None, 10):
    #     a = ser.read_until(':').decode('utf-8')
    #     print(a)

    # a = ''
    # now = time.time()
    # while ((a.find("more") < 0) and (time.time() < now + 10)):
    #     a += ser.read_until(':').decode('utf-8')
    #     time.sleep(0.01)

    # print(a)

    # self.gsm.flushInput()
    #         self.gsm.flushOutput()
    #         a = ''
    #         now = time.time()
    #         self.gsm.write(cmd+'\r\n')
    #         while (a.find(expected_reply) < 0 and a.find('ERROR') < 0 and 
    #             time.time() < now + self.REPLY_TIMEOUT):
    #             a += self.gsm.read(self.gsm.inWaiting())
    #             time.sleep(self.WAIT_FOR_BYTES_DELAY)
    lines = []
    try:
        for _ in itertools.repeat(None, 30):
            lineRaw = ser.read_until('\r').decode('utf-8')
            lines.append(lineRaw)
            print(lineRaw)
            if lineRaw.find("more") > 0:
                break
        # lineRaw = ser.read_until('...')
        # print("Line read:")
        # print(lineRaw)
        print(b'rr')
    except:
        print("Error")
        ser.close()

    for l in lines:
        ts = dt.today().strftime("%y%m%d%H%M%S")
        message_value = "{};DTM:{}$".format(l[:-1],ts).replace(" ","")
        txn.sql_txn_log(message_value)

    # while True:
    #     try:
    #         lineRaw = ser.readline().decode("utf-8")
    #         #print("RAW Serial Feed::::")
    #         #print(lineRaw)
    #         #line = lineRaw.strip().decode("utf-8")
    #         line = lineRaw.replace("\r"," ").strip()
    #     except UnicodeDecodeError:
    #         print(">> Caught UnicodeDecodeError. Skipping line")
    #         continue

    #     ts = dt.today().strftime("%y%m%d%H%M%S")
    #     message_value = "{};DTM:{}$".format(line[:-1],ts).replace(" ","") 

    #     print(message_value)

    #     # client.push_df_pub_list(message_value)
    #     txn.sql_txn_log(message_value)
    ser.close()

if __name__ == "__main__":

    try:
        relay_serial_messages()
    except KeyboardInterrupt:
        print("Bye")
