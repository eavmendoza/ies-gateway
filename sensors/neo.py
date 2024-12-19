import timeit
import time
import argparse
from datetime import datetime as dt
import neopixel, board
from rainbowio import colorwheel
import sys

import RPi.GPIO as GPIO

pixels = neopixel.NeoPixel(board.D15, 1, brightness=0.1, auto_write=False)

def get_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d","--duration", help="duration in milliseconds", type=int)
    parser.add_argument("-s","--sweep", help="sweep neopixel colors", action="store_true")
    parser.add_argument("-f","--fixed", help="fixed neopixel color", type=str, action="store")

    args = parser.parse_args()
        
    print("Duration:{}".format(args.duration))
    
    return args

def sweep(dur=0):
    wait=0.01

    start = time.monotonic()
    try:
        while True:
            for j in range(255):
                pixels.fill(colorwheel(j))
                pixels.show()
                time.sleep(wait)
            if time.monotonic()-start > dur/1000:
                break        
    except KeyboardInterrupt:
        print("Close NEO")
    finally:
        pixels.fill((0,0,0))
        pixels.show()
        return

def fixed(color="R", dur=0):
        
    out = {"R": (255,0,0),
        "G": (0,255,0),
        "B": (0,0,255),
        "W": (255,255,255),
        "Y": (255,255,0)

        }

    try:
        out[color]
    except KeyError:
        color = "W"

    start = time.monotonic()
    try:
        pixels.fill(out[color])
        pixels.show()
        while (time.monotonic()-start < dur/1000):
            pass

    except KeyboardInterrupt:
        print("Close NEO")
    finally:
        pixels.fill((0,0,0))
        pixels.show()
        return

def main():
    
    args = get_arguments()

    if args.duration == 0:
        sys.exit()

    if args.sweep:
        sweep(args.duration)

    if args.fixed:
        fixed(args.fixed, args.duration)


    GPIO.cleanup()
    
if __name__ == "__main__":
    main()