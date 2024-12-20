"""
Demo: Send Simple SMS Demo
Simple demo to send sms via gsmmodem package
"""
from __future__ import print_function

import logging

from gsmmodem.modem import GsmModem, SentSms

# PORT = 'COM5' # ON WINDOWS, Port is from COM1 to COM9 ,
# We can check using the 'mode' command in cmd
PORT = '/dev/ttyUSB0'
BAUDRATE = 9600
SMS_TEXT = 'A good teacher is like a candle, it consumes itself to light the way for others.'
SMS_DESTINATION = '09176023735'
PIN = None  # SIM card PIN (if any)


def main():
    print('Initializing modem...')
    modem = GsmModem(PORT, BAUDRATE)
    modem.connect(PIN)
    modem.waitForNetworkCoverage(10)
    print('Sending SMS to: {0}'.format(SMS_DESTINATION))

    response = modem.sendSms(SMS_DESTINATION, SMS_TEXT, True)
    if type(response) == SentSms:
        print('SMS Delivered.')
    else:
        print('SMS Could not be sent')

    modem.close()


if __name__ == '__main__':
    main()