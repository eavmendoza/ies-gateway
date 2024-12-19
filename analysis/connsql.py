from sqlalchemy import create_engine

conn={}
conn["host"] = "localhost"
conn["user"] = "iesuser"
conn["pass"] = "impbrain"
conn["schema"] = "edcslopedb"
conn["schema_props"] = "edcslopedb_properties"

# def db_conn():
# 	db_connection_str = "mysql+pymysql://{user}:{passwd}@{host}/{schema}".format(user=conn["user"], passwd=conn["pass"], host=conn["host"], schema=conn["schema"])
# 	db_connection = create_engine(db_connection_str)

# 	return db_connection

# db_connection = db_conn()
logs_db_conn = create_engine("mysql+pymysql://{user}:{passwd}@{host}/{schema}".format(user=conn["user"], passwd=conn["pass"], host=conn["host"], schema=conn["schema"]))
props_db_conn = create_engine("mysql+pymysql://{user}:{passwd}@{host}/{schema}".format(user=conn["user"], passwd=conn["pass"], host=conn["host"], schema=conn["schema_props"]))