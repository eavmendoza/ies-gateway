import timeit
import time
from dbio import txn
import argparse
import volmem.client 
from datetime import datetime as dt
from datetime import timedelta as td
import RPi.GPIO as GPIO
import subprocess as sub
from threading import Thread
from threading import Event

LAST_TIP_DT = dt.today()

class RainProps:
    def __init__(self, name=None):
        print("Getting rain props ... ", end='')
        mc = volmem.client.get()
        cnf = mc.get("gateway_config")

        if name:
            rain_name = name
        else:
            self.rain_pin = int(cnf["rain"]["pin"])
            rain_name = cnf["rain"]["name"]

        self.name = "{}-{}".format(cnf["gateway"]["name"], rain_name)
        self.mem = mc    
        print("done")


def get_coded_dt():
    return dt.today().strftime("%y%m%d%H%M%S")

def gpio_setup(rg, fprint=False):
    if fprint: print("Gpio setup ... ", end='')
    rain_pin = rg.rain_pin

    import RPi.GPIO as GPIO

    # GPIO.setmode(GPIO.BCM)
    GPIO.setmode(GPIO.BOARD)
    GPIO.setup(rain_pin, GPIO.IN, pull_up_down=GPIO.PUD_UP)
    # GPIO.setup(rain_pin, GPIO.IN)
    print(rain_pin)
    # GPIO.setup(rain_pin, GPIO.OUT)
    # GPIO.output(rain_pin, GPIO.HIGH)
    # print("Infinite loop")
    # while(True):
    #     pass

    cb = lambda channel, arg1=rg: rain_event(channel, arg1)
    # cb = rain_event
    GPIO.add_event_detect(rain_pin, GPIO.FALLING, callback=cb, bouncetime=500)  
    # GPIO.add_event_detect(rain_pin, GPIO.FALLING, callback=cb)
    if fprint: print("done")

def rain_event(channel, rg):

    global LAST_TIP_DT
    dt_event = dt.today()
    print("TIP")
    time_from_last_tip = dt_event - LAST_TIP_DT
    if time_from_last_tip.seconds < 3:
        print("Debounce")
        return
    else:
        sub.Popen(['python3', '/home/pi/gateway2/sensors/led.py', '-d500'], stdout=sub.PIPE, stderr=sub.STDOUT)
        # print(time_from_last_tip.seconds, end=" ")
        LAST_TIP_DT = dt_event

    # else record rain pulse
    rg.mem.incr("rain_count")
    dt_today_coded = get_coded_dt()
    print(dt_today_coded, rg.name)
    message_value = "{}$TIP:1;DTM:{}$".format(rg.name,dt_today_coded)
    # volmem.client.push_pub_list(message_value)

def reset_rain_count(rg):
    rg.mem.set("rain_count", 0)

def count_rain_tips(rg):
    try:
        tips = int(rg.mem.get("rain_count"))
    except (ValueError, TypeError):
        print("Error in rain count")
        return 0

    return tips

def report_rain_tips(rg, period=30):
    tips = count_rain_tips(rg)
    dt_today_coded = get_coded_dt()
    message_value = "{}$PER:{};TIP:{};DTM:{}$".format(rg.name, period, tips,
        dt_today_coded)
    print(message_value)

    # previous way of counting tips, save to memory
    # volmem.client.push_pub_list(message_value)

    # save directly to sql
    # start_time = timeit.default_timer()
    txn.sql_txn_log(message_value)
    # print("exec_time:", timeit.default_timer() - start_time)

    reset_rain_count(rg)

def report_remote_rain_tips(rg, period=30):
    tips = count_remote_rain(rg.name.split("-")[2])

    if tips==None:
        raise ValueError("Not a valid rain name. See /boot/this_gateway.cnf")

    dt_today_coded = get_coded_dt()
    message_value = "{}$PER:{};TIP:{};DTM:{}$".format(rg.name, period, tips,
        dt_today_coded)
    print(message_value)

    # previous way of counting tips, save to memory
    # volmem.client.push_pub_list(message_value)

    # save directly to sql
    # start_time = timeit.default_timer()
    txn.sql_txn_log(message_value)
    # print("exec_time:", timeit.default_timer() - start_time)

    # reset_rain_count(rg)

def get_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c","--count", help="count tips", action="store_true")
    parser.add_argument("-r","--reset", help="reset tips", action="store_true")
    parser.add_argument("-p","--report", help="report period", type=int)
    parser.add_argument("-e","--name", help="remote rain name")

    args = parser.parse_args()

    if not args.report:
        args.report = 0

    if not args.name:
        args.name = False

    return args

def set_rain_count(this_rain_gauge):
    print("Setting count ... ", end='')
    count = this_rain_gauge.mem.get("rain_count")
    if not count:
        reset_rain_count(this_rain_gauge)
    print("done")

def rain_event_monitor(rg):
    try:
        while True:
            time.sleep(300)
            GPIO.remove_event_detect(rg)
            GPIO.cleanup()
            gpio_setup(rg)
            print(".", end="")
    except KeyboardInterrupt:
        print("Bye")

def get_rain_names():
    try:
        rain_names = volmem.client.get().get("gateway_config")["remote_rain"]["names"]
    except (KeyError, ValueError):
        print("ERROR: Rain names not in config")
        return None

    rain_names = rain_names.upper()
    return rain_names.split(",")

# def report_remote_rain(name):
def count_remote_rain(name, period=10, current_period=False):
    
    name = name.upper()
    rain_names = get_rain_names()
    print("Rain names:", rain_names)

    if name not in rain_names:
        print("ERROR: name \"{name}\" is not in config".format(name=name))
        return None

    start_dt = dt.today()
    if current_period:
        minute_delay = start_dt.minute%period
    else:
        minute_delay = start_dt.minute%period+period
    start_dt = start_dt - td(minutes=minute_delay,
        seconds=start_dt.second)
    print(start_dt)

    # query = ("select count(*) from (select * from instantaneous_rain "
    #     "where message like '%{name}%' "
    #     "and dt > '{start_dt}'"
    #     "group by unix_timestamp(dt) div 10) tb").format(name=name,
    #     start_dt=start_dt.strftime("%Y-%m-%d %H:%M:%S"))

    query = ("select count(*) from (select * from instantaneous_rain "
        "where message like '%{name}%' and message like '%tip%' "
        "and dt > '{start_dt}'"
        "group by unix_timestamp(dt) div 10) tb").format(name=name,
        start_dt=start_dt.strftime("%Y-%m-%d %H:%M:%S"))

    return txn.read(query)[0][0]
    # print(val)

def main():
    args = get_arguments()

    this_rain_gauge = RainProps()

    if args.report > 0:
        print("Reporting rain")
        if args.name:
            remote_rain = RainProps(name=args.name)
            report_remote_rain_tips(remote_rain, args.report)
        else:
            report_rain_tips(this_rain_gauge, args.report)
        return

    if args.reset:
        print("Resetting rain count ... ", end='')
        reset_rain_count(this_rain_gauge)
        print("done")
        return

    if args.count:
        if args.name:
            name = args.name.upper()
            tips = count_remote_rain(name=args.name,current_period=True)
        else:
            name = "Default"
            tips = count_rain_tips(this_rain_gauge)
            
        print("RG: {name}, Rain tips: {tips}".format(tips=tips,
            name=name))
            
        return

    set_rain_count(this_rain_gauge)
    thread1 = Thread(target=gpio_setup(this_rain_gauge, True))
    thread1.start()
    # rain_event_monitor(this_rain_gauge)

    cycle=0
    # no_rain_count=0
    while (cycle<12):
        time.sleep(300)
        print("I'm alive: {}".format(dt.today().strftime("%x %X")), no_rain_cout)
        # no_rain_count += 1
        # if (no_rain_count>30):
        #     print("Resetting..")
        #     GPIO.cleanup()
        #     break


    GPIO.cleanup()
    
if __name__ == "__main__":
    main()




