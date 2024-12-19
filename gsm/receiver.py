import modem
import argparse
from datetime import datetime as dt
from datetime import timedelta as td
from dbio import txn
import time

def get_arguments():
    """
    -The function that checks the argument that being sent from main function and returns the
    arguement of the function.
    Returns: 
        dict: Mode of action from running python **-db,-ns,-b,-r,-l,-s,-g,-m,-t**.
     
    Example Output::
         >> print args.dbhost
           gsm2 #Database host it can be (local or gsm2)
         >> print args.table
           loggers #Smsinbox table (loggers or users)
         >> print args.mode
            #Mode id
         >> print args.gsm
            globe #GSM name (globe1, smart1, globe2, smart2)*
         >> print args.status**
            2 #GSM status of inbox/outbox#
         >> print args.messagelimit**
            5000 #Number of message to read in the process
         >> print args.runtest**
            #Default value False. Set True when running a test in the process
         >> print args.bypasslock**
            #Default value False
         >> print args.nospawn
            #Default value False
    """
    parser = argparse.ArgumentParser(description = ("Run SMS parser\n "
        "smsparser [-options]"))
    parser.add_argument("-o", "--dbhost", 
        help="host name (check server config file")
    parser.add_argument("-c", "--sms_data_resource", 
        help="sms data resource name (check server config file")
    parser.add_argument("-e", "--sensor_data_resource", 
        help="sensor data resource name (check server config file")
    parser.add_argument("-t", "--table", help="smsinbox table")
    parser.add_argument("-m", "--mode", help="mode to run")
    parser.add_argument("-g", "--gsm", help="gsm name")
    parser.add_argument("-s", "--status", help="inbox/outbox status", type=int)
    parser.add_argument("-l", "--messagelimit", 
        help="maximum number of messages to process at a time", type=int)
    parser.add_argument("-r", "--runtest", 
        help="run test function", action="store_true")
    parser.add_argument("-b", "--bypasslock", 
        help="bypass lock script function", action="store_true")
    parser.add_argument("-ns", "--nospawn", 
        help="do not spawn alert gen", action="store_true")
    
    try:
        args = parser.parse_args()

        if args.dbhost == None:
            args.dbhost = 'local'
        print ("Host: %s" % args.dbhost)
        
        print ("Table: %s" % args.table)

        if args.status == None:
            args.status = 0
        print ("Staus to read: %s" % args.status)


        if args.messagelimit == None:
            args.messagelimit = 200
        print ("Message limit: %s" % args.messagelimit)

        return args        
    except IndexError:
        print ('>> Error in parsing arguments')
        error = parser.format_help()
        print (error)
        sys.exit()

def main():
    print("GSM Receive Server")

    gsm = modem.GsmModem('/dev/ttyUSB0', 9600, 29, 22)
    gsm.set_defaults()

    while(1):
        msg_list = gsm.get_all_sms()

        if msg_list:
            for m in msg_list:
                # print("####################")
                txn.sql_txn_log(msg=m.data, table="message_transactions", feed_name="gsm", 
                    dbname="db_remote")
                # print("\n\n")
            gsm.delete_sms(2)
            time.sleep(10)
        else:
            print(".",end="")
            time.sleep(30)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Exiting")