import subprocess as sub

sub.Popen(['sudo','python3.6', '/home/pi/gateway2/sensors/neo.py', '-s', '-d10000'], stdout=sub.PIPE, stderr=sub.STDOUT)
