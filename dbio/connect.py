def connect():
    import MySQLdb
    from volmem import client
    import time
    conn = client.get().get("cnf")["mysql"]
    print(conn["host"], conn["user"], conn["pwd"], conn["schema"])
    try:
        db = MySQLdb.connect(conn["host"], conn["user"], 
            conn["pwd"], conn["schema"])
        cur = db.cursor()
        return db, cur
    except TypeError:
        print("Error Connection Value")
        return False
    except (MySQLdb.Error, MySQLdb.Warning) as e:
        print(e)
        return False
    
def connsql_old(cfg):
    from sqlalchemy import create_engine
    conn={}
    # user = cfg['user']
    conn["logs"]=create_engine(f"mysql+pymysql://{cfg['user']}:{cfg['pass']}@{cfg['host']}/{cfg['schema']}")
    conn["props"]=create_engine(f"mysql+pymysql://{cfg['user']}:{cfg['pass']}@{cfg['host']}/{cfg['schema_props']}")
    
    return conn

def connsql(db_type="logs"):
    import yaml, sys
    from pathlib import Path as path
    import mysql.connector
    # cred_file = str(path.home())+'/gateway/setup/db_cred.yaml'
    cred_file = '/home/ies/gateway/setup/db_cred.yaml'
    creds = yaml.safe_load(open(cred_file))['DB']

    conn={}
    conn["host"] = creds['Host']
    conn["user"] = creds['User']
    conn["pass"] = creds['Password']
    conn["schema"] = creds['DbName']
    conn["schema_props"] = creds['DbName_props']

    if (db_type=="props"):
        props_db_conn = create_engine("mysql+pymysql://{user}:{passwd}@{host}/{schema}".format(user=conn["user"], passwd=conn["pass"], host=conn["host"], schema=conn["schema_props"]))
        return {"props": props_db_conn}
    else:
        logs_db_conn = mysql.connector.connect(user=conn["user"], password=conn["pass"],
                                  host=conn["host"], database=conn["schema"])

        return {"logs": logs_db_conn}



def write(query=None):
    if not query:
        raise ValueError("No query defined")

    ret_val = None
    caller_func = str(inspect.stack()[1][3])
    db, cur = connect()

    try:
        a = cur.execute(query)
        db.commit()
    except IndexError:
        print("IndexError on ", inspect.stack()[1][3])
    except (MySQLdb.Error, MySQLdb.Warning) as e:
        print(e, caller_func)
    finally:
        db.close()
        return ret_val

def read(query=''):
    import MySQLdb
    if not query:
        raise ValueError("No query defined")
    
    ret_val = None
    caller_func = str(inspect.stack()[1][3])
    db, cur = connect()
    try:
        a = cur.execute(query)
        try:
            a = cur.fetchall()
            ret_val = a
        except ValueError:
            ret_val = None
    except MySQLdb.OperationalError:
        print("MySQLdb.OperationalError on ", caller_func)
    except (MySQLdb.Error, MySQLdb.Warning) as e:
        print(e, caller_func)
    except KeyError:
        print("KeyError on ", caller_func)
    finally:
        db.close()
        return ret_val