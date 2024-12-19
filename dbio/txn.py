
import MySQLdb
from volmem import client
import time
import inspect
from datetime import datetime as dt

def connect(dbname="db_local"):
    conn = client.get().get("gateway_config")[dbname]
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

def write(query=None, dbname="db_local"):
    if not query:
        raise ValueError("No query defined")

    ret_val = None
    caller_func = str(inspect.stack()[1][3])
    db, cur = connect(dbname=dbname)

    try:
        a = cur.execute(query)
        ret_val = True
        db.commit()
    except IndexError:
        print("IndexError on ", inspect.stack()[1][3])
    except (MySQLdb.Error, MySQLdb.Warning) as e:
        print(e, caller_func)
    finally:
        db.close()
        return ret_val

def read(query=''):
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

def sql_txn_log(msg, stat=0, dbname="db_local", table="transactions"):
    dt_today = dt.today().strftime("%Y-%m-%d %H:%M:%S")

    if dbname == "db_remote":
        query = ("insert into {} (transaction_datetime, message, feed_name, transaction_type) "
            "values ('{}','{}','{}',{})".format("message_transactions", dt_today, msg, "gatewaylog", 1))
    else:
        query = ("insert into {} (dt, message, stat) "
            "values ('{}','{}','{}')".format(table, dt_today, msg, stat))

    return write(query=query, dbname=dbname)

def txn_log(msg, conn, log_type):
    import mysql.connector
    dt_today = dt.today().strftime("%Y-%m-%d %H:%M:%S")

    query = (f"insert into message_transactions \
        (transaction_datetime, message, feed_name, transaction_type)\
        values (%s,%s,%s,%s)")
    # print(query)
    val = (dt_today, msg, log_type, 1)

    cur = conn.cursor()

    cur.execute(query, val)

    return conn.commit()
