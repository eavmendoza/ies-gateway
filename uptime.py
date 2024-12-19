# -*- coding: utf-8 -*-
"""
Created on Fri Jun 21 09:33:09 2024

@author: earlm
"""

# -*- coding: utf-8 -*-
"""
Created on Mon Jun 17 13:25:43 2024

@author: earlm
"""
import time
import pandas as pd
import re
from analysis.dtdef import DateWindow, today
import argparse


def get_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d","--days_span", help="days span")
    parser.add_argument("-e","--end_day", help="end day")
    parser.add_argument("-s","--start_day", help="start day")
    parser.add_argument("-b","--bu_id", help="business unit id", type=int)
    parser.add_argument("-c","--save_to_csv", help="save to csv", action='store_true')
    parser.add_argument("-t","--type", help="plot type")
    parser.add_argument("-r","--real_time", help="reflect real time", action='store_true')
    parser.add_argument("-f","--file", help="reflect real time", type=str)

    args = parser.parse_args()

    if not args.bu_id:
        # generate plots for all sites
        args.bu_id = list(range(1,5))
    else:
        args.bu_id = [args.bu_id]

    if not args.end_day:
        args.end_day = today()
        args.real_time = True
    else:
        args.real_time = False
        # print(args.end_day.dt)

    if not args.days_span and not args.end_day:
        args.days_span = 14

    if not args.type:
        args.type="all"
        


    return args
   
BU_NAME={1:"BGBU", 2:"LGBU", 3:"NIGBU", 4:"MAGBU"} 

if __name__ == "__main__":
    
    args=get_arguments()
    if args.days_span:
        days_span=int(args.days_span)
    else:
        days_span=None
    window = DateWindow(days_span=days_span, end_day=args.end_day, start_day=args.start_day)
    
    from dbio.connect import connsql
    from analysis.settings import get_config
    cfg = get_config()
    conn = connsql(cfg['localdb'])
    
    from analysis import sensors
    
    for bu_id in args.bu_id:
        print(f"Exporting {BU_NAME[bu_id]} data from {window.start.string} to {window.end.string}")
        
        f_args = (bu_id, window, config, args.real_time)
        print(f_args)
        # if (args.type=="all"):
        #     Thread(target=update_network_dash, args=f_args).start()
        #     Thread(target=update_rainfall_dash, args=f_args).start()
        #     Thread(target=update_tilt_dash, args=f_args).start()
        #     Thread(target=update_soms_dash, args=f_args).start()
        #     Thread(target=update_power_dash, args=f_args).start()

        # else:
        #     if ("net" in args.type):        Thread(target=update_network_dash, args=f_args).start()
        #     if ("rain" in args.type):       Thread(target=update_rainfall_dash, args=f_args).start()
        #     if ("tilt" in args.type):       Thread(target=update_tilt_dash, args=f_args).start()
        #     if ("soms" in args.type):       Thread(target=update_soms_dash, args=f_args).start()
        #     if ("power" in args.type):      Thread(target=update_power_dash, args=f_args).start()

        # print("Thread spawned")
    
    print("done")