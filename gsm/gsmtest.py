import os
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import modem

gsm = modem.GsmModem('/dev/ttyUSB0', 9600, 29, 22)
gsm.set_defaults()
gsm.send_msg("Porbida!!@#@$%*(@(*#$(*!!]}{}[][]", "09176023735")
# all_sms = gsm.get_all_sms()

# for sms in all_sms:
# 	print("*****")
# 	print(sms.data)
# 	print(sms.simnum)
# print (gsm.reset())