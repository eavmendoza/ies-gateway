# SPDX-FileCopyrightText: 2021 ladyada for Adafruit Industries
# SPDX-License-Identifier: MIT

# Simple test for NeoPixels on Raspberry Pi
import time
import board
import neopixel
import argparse

# Choose an open pin connected to the Data In of the NeoPixel strip, i.e. board.D18
# NeoPixels must be connected to D10, D12, D18 or D21 to work.
pixel_pin = board.D21

# The number of NeoPixels
num_pixels = 1

# The order of the pixel colors - RGB or GRB. Some NeoPixels have red and green reversed!
# For RGBW NeoPixels, simply change the ORDER to RGBW or GRBW.
# ORDER = neopixel.GRB

pixels = neopixel.NeoPixel(
    pixel_pin, num_pixels, brightness=0.2
)

pixels[0] = (10,10,10)
MODES = [
		( 0, 0, 0),
		(10,10,10), # activity
		( 0, 0,10) 	# rain gauge
		]


def get_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d","--duration", help="duration in milliseconds", type=int)
    parser.add_argument("-m","--mode", help="mode", type=int)

    args = parser.parse_args()
        
    print("Duration:{}".format(args.duration))
    
    return args

def main():
    
    args = get_arguments()

    if args.duration == 0:
        sys.exit()

    try:
        # while True:
        pixels[0] = MODES[args.mode]
        time.sleep(args.duration/1000)
        
        pixels[0] = MODES[0]
        time.sleep(0.1)

    except KeyboardInterrupt:
        print("Bye")
    
if __name__ == "__main__":
    main()