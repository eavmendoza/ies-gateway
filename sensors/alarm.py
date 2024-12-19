import timeit
import time
from dbio import txn
import argparse
import volmem.client 
import volmem.client as client
from datetime import datetime as dt


import RPi.GPIO as GPIO

mc = client.get()
cnf = mc.get("gateway_config")
ALARM_PIN = int(cnf["alarm"]["pin"])

GPIO.setmode(GPIO.BOARD)
GPIO.setup(ALARM_PIN, GPIO.OUT)
print("PIN:", ALARM_PIN)

GPIO.setmode(GPIO.BOARD)
GPIO.setup(ALARM_PIN, GPIO.OUT)
print(ALARM_PIN)

def get_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d","--duration", help="duration in seconds", type=int)

    args = parser.parse_args()
        
    print("Duration={}".format(args.duration))
    
    return args

def main():
    
    args = get_arguments()

    if args.duration == 0:
        sys.exit()

    try:
        # while True:
        print("HIGH")
        GPIO.output(ALARM_PIN, GPIO.HIGH)
        time.sleep(args.duration)
        
        print("LOW")
        GPIO.output(ALARM_PIN, GPIO.LOW)
        # time.sleep(5)

    except KeyboardInterrupt:
        print("Bye")
    
    GPIO.cleanup()
    
if __name__ == "__main__":
    main()