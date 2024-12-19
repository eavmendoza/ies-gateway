import sys
import RPi.GPIO as GPIO
import subprocess as sub
import time

switch_pin = 15
GPIO.setmode(GPIO.BOARD)
GPIO.setup(switch_pin, GPIO.IN)

try:
    while (True):
        switch_status = GPIO.input(switch_pin)

        if (switch_status==0):
            time.sleep(1)
            if (switch_status==0):
                print('Shutting down')
                sub.Popen(['sudo','python3.6', '/home/pi/gateway2/sensors/neo.py', '-s', '-d3'], stdout=sub.PIPE, stderr=sub.STDOUT)
                # sub.Popen(['sudo','python3.8', '/home/pi/gateway2/sensors/indicator.py', 
                #     '-d500','-m1'], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                command = "/usr/bin/sudo /sbin/shutdown -h now"
                process = sub.Popen(command.split(), stdout=sub.PIPE)
                output = process.communicate()[0]
                sys.exit()

except KeyboardInterrupt:
	GPIO.cleanup()
	print("Out")