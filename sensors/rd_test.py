# SPDX-FileCopyrightText: 2021 ladyada for Adafruit Industries
# SPDX-License-Identifier: MIT

# Example to send a packet periodically between addressed nodes
# Author: Jerry Needell
#
import time
import board
import busio
import digitalio
import adafruit_rfm9x


# set the time interval (seconds) for sending packets
transmit_interval = 10

# Define radio parameters.
RADIO_FREQ_MHZ = 433.0  # Frequency of the radio in Mhz. Must match your
# module! Can be a value like 915.0, 433.0, etc.

# Define pins connected to the chip.
CS = digitalio.DigitalInOut(board.CE1)
RESET = digitalio.DigitalInOut(board.D25)

# Initialize SPI bus.
spi = busio.SPI(board.SCK, MOSI=board.MOSI, MISO=board.MISO)
# Initialze RFM radio
rfm9x = adafruit_rfm9x.RFM9x(spi, CS, RESET, RADIO_FREQ_MHZ)

# initialize counter
counter = 0

# rfm9x.signal_bandwidth = 62500
# rfm9x.spreading_factor = 9
# rfm9x.coding_rate = 8
# enable CRC checking
rfm9x.enable_crc = True
# set delay before sending ACK
rfm9x.ack_delay = 0.1
# set node addresses
rfm9x.node = 0
rfm9x.destination = 2

# send a broadcast message from my_node with ID = counter
rfm9x.send(
    bytes("Startup message {} from node {}".format(counter, rfm9x.node), "UTF-8")
)
rfm9x.ack_delay=0.1

print("FREQ:", rfm9x.frequency_mhz)
print("BW:",rfm9x.signal_bandwidth)
print("CR:", rfm9x.coding_rate)
print("SF: ", rfm9x.spreading_factor)
print("Receive timeout", rfm9x.receive_timeout)


# Wait to receive packets.
print("Waiting for packets...")
now = time.monotonic()
while True:
    # Look for a new packet: only accept if addresses to my_node
    packet = rfm9x.receive(with_ack=True, keep_listening=True, with_header=True)
    # packet = rfm9x.receive(with_header=True, with_ack=True, keep_listening=True)
    # If no packet was received during the timeout then None is returned.
    if packet is not None:
        # Received a packet!
        # Print out the raw bytes of the packet:
        print("Received (raw header):", [hex(x) for x in packet[0:4]])
        print("Received (raw payload): {0}".format(packet[4:]))
        print("Received RSSI: {0}".format(rfm9x.last_rssi))
        print("Packet: ", packet[4:].decode("utf-8", "replace"))
        rfm9x.destination = packet[1]
        rfm9x.send_with_ack(
            bytes(
                "AAA".format(counter, rfm9x.node), "UTF-8"
            ),
            # keep_listening=True,
            # destination=packet[0]
        )
    # if time.monotonic() - now > transmit_interval:
    #     now = time.monotonic()
    #     counter = counter + 1
    #     # send a  mesage to destination_node from my_node
    #     rfm9x.send(
    #         bytes(
    #             "message number {} from node {}".format(counter, rfm9x.node), "UTF-8"
    #         ),
    #         keep_listening=True,
    #         destination=1
    #     )
    #     # button_pressed = None