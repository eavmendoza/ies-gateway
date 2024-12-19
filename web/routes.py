from FlaskApp import app
from FlaskApp import create_dash_application
import logging
import time
import subprocess
import select
import shlex
import re

@app.route('/rain')
def rain():
    # command = "python3 /home/pi/gateway2/sensors/rainwatch.py -c"
    # command = "python3 -c 'print(hello python)'"
    command = "sh /home/pi/raincount.sh"
    p = subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE)
    text = p.communicate()

    return str(text)

    # try:
    #     tips=re.search("(?<=tips: )\d+",str(text)).group(0)
    # except:
    #     return "ERROR in parsing rain return"+str(text)

    # tips=re.search("(?<=tips: )\d+",str(text)).group(0)

    htmlout = """
        <!DOCTYPE html>
        <html>
        <body>
    """

    htmlout += "<h1>Rain tips: </h1><br />"
    htmlout += "<p><font size=10>"
    htmlout += str(tips)
    htmlout += "<font></p>"
    htmlout += """
        </body>
        </html>
    """

    return(htmlout)

@app.route('/raw')
def homepage():
    command = "awk '{a[i++]=$0} END {for (j=i-1; j>=i-50;) print a[j--] }' '/home/pi/lora.logs'"
    p = subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE)
    text = p.communicate()[0]

    htmlout = """
        <!DOCTYPE html>
        <html>
        <body>
    """

    htmlout += "<h1>Transmissions: </h1><br />"
    htmlout += "<p>"
    htmlout += str(text).replace('\\n','<br />').replace('\\x00','').replace('[','<b>').replace(']','</b>')
    htmlout += "</p>"
    htmlout += """
        </body>
        </html>
    """

    return(htmlout)