from crontab import CronTab
from screenutils import list_screens, Screen
import re
import os
import subprocess
import argparse
import sys

def execute_cmd(cmd, wait_for_out=True):
    my_env = os.environ.copy()
    my_env["PATH"] = "/home/pi/gateway2:" + my_env["PATH"]
    my_env["PYTHONPATH"] = "/home/pi/gateway2"
    my_env["USER"] = 'pi'
    my_env["USERNAME"] = 'pi'
    p = subprocess.Popen(cmd, stdout = subprocess.PIPE, 
        shell = True, stderr = subprocess.STDOUT,
        env = my_env
        )

    if wait_for_out:
        out, err = p.communicate()
        print("out: {}, err: {}".format(out, err))
        return out, err
    else:
        return

def get_jobs():
    gateway_cron  = CronTab(user='pi')

    screen_jobs = {}

    for job in gateway_cron:
        if "screen" in repr(job):
            screen_name = re.search("(?<=Sdm )\w+(?= python3)", repr(job)).group(0)
            screen_jobs[screen_name] = re.search("screen.+(?=$)", job.command).group(0)

    return screen_jobs

def run_daemons():
    screen_jobs  = get_jobs()

    for name in screen_jobs.keys():
        if not Screen(name).exists:
            print("Running {}".format(name))
            execute_cmd(screen_jobs[name])
        else:
            print("{} is running".format(name))

def kill_daemons():
    for screen in list_screens():
        print("Killing {}".format(screen))
        screen.kill()


def get_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-r","--run", help="run jobs", action="store_true")
    parser.add_argument("-k","--kill", help="force kill jobs", action="store_true")

    args = parser.parse_args()

    return args


if __name__ == "__main__":
    args = get_arguments()

    if args.kill:
        kill_daemons()

    if args.run:
        run_daemons()
        sys.exit()

