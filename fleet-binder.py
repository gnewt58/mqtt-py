#!/usr/bin/python
import datetime
import optparse
import json
import paho.mqtt.client as mqtt
import mysql.connector
from mysql.connector import errorcode


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
  if options.debug:
    print "this is on_connect. Connection result: "+mqtt.connack_string(rc)
  # Subscribing in on_connect() means that if we lose the connection and
  # reconnect then subscriptions will be renewed.
  client.subscribe("bind/request")

def on_disconnect(client, userdata, rc):
  if rc != 0:
    print("Unexpected disconnection:"+rc)

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
  if options.debug:
    print "this is on_message. t: "+str(msg.topic)+" p: "+str(msg.payload)
  # search pseudobind table in database
  # Connect to mysql on local host
  try:
    cnx = mysql.connector.connect(user=creds["mysql"][0]["user"],password=creds["mysql"][0]["password"],
                                    database='mosquitto_fleet')
  except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
      print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
      print("Database does not exist")
    else:
      print(err)

  cursor = cnx.cursor()

  search_cid = str(msg.payload)
  query = ("SELECT * FROM pseudobind WHERE cid = '" + search_cid + "'")
  cursor.execute(query)

    # We expect only a single result as this is a unique field (cid)
  row = cursor.fetchone()
  if row is not None:
    #row = rows[0]
    found_devid = row[1]
    #print ("I found device ID [" + found_devid + "]")
    update_stmt = "UPDATE pseudobind SET last_update = '"+ str(datetime.datetime.now()) +"', active = 1 WHERE cid = '" + search_cid + "'"
    if options.debug:
      print (update_stmt)
    cursor.execute(update_stmt)
    cnx.commit()
  else:
    #cursor.fetchall() # clear the cursor for the insert statement
    found_devid = "disco-"+search_cid
    insert_stmt = "INSERT INTO pseudobind (cid, devid, description, last_update, active) VALUES ('" + search_cid + "','" + found_devid + "','<discovered device>','" + str(datetime.datetime.now()) + "', '1')"
    if options.debug:
      print (insert_stmt)
    #data = (search_cid, found_devid, "<discovered device>", right_now )
    #data = ( right_now )
    cursor.execute(insert_stmt)
    cnx.commit()
  cursor.close()
  cnx.close()

  #print("pub: bind/"+search_cid+" "+str(found_devid))
  client.publish("bind/"+search_cid, str(found_devid) )

## Get command line option(s)
## currently only looking for '-d/--debug' for debug
parser = optparse.OptionParser()
parser.add_option('-d', '--debug',
                  dest="debug",
                  default=False,
                  action="store_true",
                  )
options, remainder = parser.parse_args()

## Get credentials from json formatted file 'credentials'
with open('credentials', 'r') as file:
  jcreds = file.read().replace('\n','')
creds = json.loads(jcreds)
if options.debug:
  print ("mqttuser = ["+creds["mqtt"]["user"]+"], password = ["+creds["mqtt"]["password"]+"]")
  print ("mysqluser1 = ["+creds["mysql"][0]["user"]+"], password = ["+creds["mysql"][0]["password"]+"]")
  print ("mysqluser2 = ["+creds["mysql"][1]["user"]+"], password = ["+creds["mysql"][1]["password"]+"]")
client = mqtt.Client("fleet-binder")
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message = on_message
client.username_pw_set(creds["mqtt"]["user"],creds["mqtt"]["password"])
client.connect("localhost",1883,60)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()
