from Adafruit_IO import MQTTClient
import time
import sys


# ADAFRUIT_IO_KEY      = mqtt_config["key"]
# ADAFRUIT_IO_USERNAME = mqtt_config["username"]
# THROTTLE_FEED_ID = mqtt_config["feed"]

class DisconnectException(Exception):
    pass

class ThrottleException(Exception):
    pass

def publish(feed=None, value="", username="", key=""):
    start = time.time()
    mqtt_client = MQTTClient(username, key)
    mqtt_client.connect()
    # print("Connection time: {0}".format(time.time() - start))

    print(f'Publishing {feed} {value}', end=" ")
    # start = time.time()
    mqtt_client.publish(feed, value)
    print("done")
    # print("Publish time: {0}".format(time.time() - start))

def disconnected(client):
   # Disconnected function will be called when the client disconnects.
   print('Disconnected from Adafruit IO!')
   raise DisconnectException

def message(client, feed_id, payload):
    # Message function will be called when a subscribed feed has a new value.
    # The feed_id parameter identifies the feed, and the payload parameter has
    # the new value.
    # raise ThrottleException(payload)

    print('Feed {0} received new value: {1}'.format(feed_id, payload))

def connected(client):
    # Connected function will be called when the client is connected to Adafruit IO.
    # This is a good place to subscribe to feed changes.  The client parameter
    # passed to this function is the Adafruit IO MQTT client so you can make
    # calls against it easily.
    # print("Connected to Adafruit IO!  Listening for {0} "
    # "changes...".format(THROTTLE_FEED_ID))
    # Subscribe to changes on a feed named DemoFeed.
    # client.subscribe(THROTTLE_FEED_ID)
    print("Connected to Adafruit IO! ")

def get_client(username, key):
    mqtt_client = MQTTClient(username, key)
    mqtt_client.on_disconnect = disconnected
    mqtt_client.on_message = message
    mqtt_client.on_connect = connected
    

    mqtt_client.connect()
    mqtt_client.loop_background()

    print("Connected to remote mqtt server")

    return mqtt_client