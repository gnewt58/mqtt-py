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
  client.subscribe("sensors/#")

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
      
  if options.debug:
    print(msg.topic+" -> "+msg.payload)
  splittopic = [x.strip() for x in msg.topic.split("/")]
  insert_stmt = "INSERT INTO datalogger (devid,sensorid,value,units) VALUES ('"+splittopic[1]+"','"+splittopic[2]+"','"+msg.payload+"','"+splittopic[3]+"')"
  #"INSERT Ime.datetime.now())+"','"+splittopic[0]+"','"+splittopic[1]+"','"+msg.payload+"','"+splittopic[2]+"')"
  if options.debug:
    print(insert_stmt)
  cursor.execute(insert_stmt)
  cnx.commit()

  ### Insert / update latest reading table
  search_stmt = "SELECT * FROM latest WHERE (devid,sensorid) = ('"+splittopic[1]+"','"+splittopic[2]+"')"
  cursor.execute(search_stmt)

  # We expect only a single result as this is a unique field (cid)
  row = cursor.fetchone()
  if row is None:
    sql_stmt = "INSERT INTO latest (devid,sensorid,value,units) VALUES ('"+splittopic[1]+"','"+splittopic[2]+"','"+msg.payload+"','"+splittopic[3]+"')"

  else:
    # cursor.fetchall()
    sql_stmt = "UPDATE latest SET value = '"+ msg.payload +"' WHERE (devid,sensorid) = ('"+splittopic[1]+"','"+splittopic[2]+"')"
  
  cursor.execute(sql_stmt)
  cnx.commit()
  
  cursor.close()
  cnx.close()


#-----------------------------------------------------------------------------------
# MAIN
#-----------------------------------------------------------------------------------
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
print ("mqttuser = ["+creds["mqtt"]["user"]+"], password = ["+creds["mqtt"]["password"]+"]")
print ("mysqluser = ["+creds["mysql"][0]["user"]+"], password = ["+creds["mysql"][0]["password"]+"]")
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.username_pw_set(creds["mqtt"]["user"],creds["mqtt"]["password"])
client.connect("localhost", 1883, 60)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()
