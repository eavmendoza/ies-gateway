import time
import RPi.GPIO as GPIO
GPIO.setmode(GPIO.BOARD)

btn_input = 18;     # button to monitor for button presses.
LED_output = 17;   # LED to light or not depending on button presses.

# GPIO btn_input set up as input.
GPIO.setup(btn_input, GPIO.IN, pull_up_down=GPIO.PUD_UP)
# GPIO.setup(LED_output, GPIO.OUT)

# handle the button event
def buttonEventHandler_rising (pin):
    # turn LED on
    # GPIO.output(LED_output,True)
    raise Exception('button pressed')
    
def buttonEventHandler_falling (pin):
    # turn LED off
    # GPIO.output(LED_output,False)
    raise Exception('button released')


# set up the event handlers so that when there is a button press event, we
# the specified call back is invoked.
# for your purposes you may only want to detect the falling
# indicating that the button was released.
# GPIO.add_event_detect(btn_input, GPIO.RISING, callback=buttonEventHandler_rising) 
GPIO.add_event_detect(btn_input, GPIO.FALLING, callback=buttonEventHandler_falling)
 
# we have now set our even handlers and we now need to wait until
# the event we have registered for actually happens.
# This is an infinite loop that waits for an exception to happen and
# and when the exception happens, the except of the try is triggered
# and then execution continues after the except statement.
try:  
    while True :
        time.sleep(5)
        print(".")
except:
    GPIO.cleanup()   