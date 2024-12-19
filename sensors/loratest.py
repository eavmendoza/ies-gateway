"""
Wiring Check, Pi Radio w/RFM9x

Learn Guide: https://learn.adafruit.com/lora-and-lorawan-for-raspberry-pi
Author: Brent Rubell for Adafruit Industries
"""
import time
import busio
from digitalio import DigitalInOut, Direction, Pull
import board
# Import the RFM9x radio module.
import adafruit_rfm9x

# Configure RFM9x LoRa Radio
CS = DigitalInOut(board.CE1)
RESET = DigitalInOut(board.D25)
spi = busio.SPI(board.SCK, MOSI=board.MOSI, MISO=board.MISO)

try:
  rfm9x = adafruit_rfm9x.RFM9x(spi, CS, RESET, 433.0)
  rfm9x.tx_power = 23
  print('RFM9x: Detected')
except RuntimeError as error:
  # Thrown on version mismatch
  print('RFM9x: ERROR')
  print('RFM9x Error: ', error)

while True:
  payload = "Hello"
  button_a_data = bytes(payload,"utf-8")
  rfm9x.send(button_a_data)
  print(payload)

  time.sleep(2)