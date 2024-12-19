from configparser import ConfigParser
from volmem import client
import os

def set():
    cfile = os.path.dirname(os.path.realpath(__file__)) + "/this_gateway.cnf"
    mem = client.get()
    config = ConfigParser()
    print(config.read(cfile))
    mem.set("gatewaycnf",config)
    return config

if __name__ == "__main__":
    set()
