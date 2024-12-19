import timeit
import time
import argparse
from volmem import client
from datetime import datetime as dt
import sys

import RPi.GPIO as GPIO

mc = client.get()
cnf = mc.get("gateway_config")
LED_PIN = int(cnf["led"]["pin"])

GPIO.setmode(GPIO.BOARD)
GPIO.setup(LED_PIN, GPIO.OUT)
print("PIN:", LED_PIN)

def get_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d","--duration", help="duration in milliseconds", type=int)

    args = parser.parse_args()
        
    print("Duration:{}".format(args.duration))
    
    return args




def main():
    
    args = get_arguments()

    if args.duration == 0:
        sys.exit()

    try:
        # while True:
        print("Status:ON")
        GPIO.output(LED_PIN, GPIO.HIGH)
        time.sleep(args.duration/1000)
        
        print("Status:OFF")
        GPIO.output(LED_PIN, GPIO.LOW)
        # time.sleep(5)

    except KeyboardInterrupt:
        print("Bye")
    
    GPIO.cleanup()
    
if __name__ == "__main__":
    main()