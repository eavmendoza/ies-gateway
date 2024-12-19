# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import re
import pandas as pd
from datetime import datetime as dt

f = open("C:/Users/earlm/Downloads/edcrpidb_LTC_20240605.sql")

l_data = []

for line in f.readlines():
    if (len(line.split(","))<5):
        continue
    
    ts = re.search("\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}",line)
    msg = re.search("(?<=\')L.+(?=\')",line)
    
    if not (dt and msg):
        continue
    
    l_data.append({
        "ts": dt.strptime(ts.group(0), "%Y-%m-%d %H:%M:%S"),
        'msg': msg.group(0)
    })
        
        
f.close()

df = pd.DataFrame.from_records(l_data, index=['ts'])
df = df.loc[df.index>'2024-06-01']
