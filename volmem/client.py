import imp
import datetime
dt = datetime.datetime
td = datetime.timedelta
import pylibmc

def get():
    return pylibmc.Client(["127.0.0.1"], binary=True,
        behaviors={"tcp_nodelay": True, "ketama": True})

def push_pub_list(message_value):
    mc = get()
    pub_list = mc.get("pub_list")
    if not pub_list:
        pub_list = []
    pub_list.append(message_value)
    mc.set("pub_list", pub_list)

def print_memory(valuestr):
    sms_df = get().get(valuestr)
    print_mem_df(sms_df)

def print_mem_df(sms_df):
    for index, row in sms_df.iterrows():
        print("Index:", index)
        print("Timestamp:", sms_df.loc[index, 'ts'])
        print("Status:", sms_df.loc[index, 'stat'])
        print("Message:", sms_df.loc[index, 'msg'])
        print("")

def push_df_pub_list(message):
    mc_client = get()
    df_pub_list = mc_client.get("df_pub_list")

    data = {"ts": [dt.today()],  "msg": [message], "stat" : [0]}
    
    df_pub_list = df_pub_list.append(pd.DataFrame(data), 
        ignore_index = True, sort=True)

    mc_client.set("df_pub_list", df_pub_list)


def reset_memory(valuestr, force_reset=False):
    storage_column_names = ["ts","msg","stat"]
    mc = get()
    value_pointer = mc.get(valuestr)

    if (value_pointer is None) or (force_reset):
        value_pointer = pd.DataFrame(columns = storage_column_names)
        mc.set(valuestr,value_pointer)      
        print("set %s as empty object" % (valuestr))
    else:
        print(value_pointer)