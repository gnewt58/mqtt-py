#!/usr/bin/python
import datetime
import optparse
import json
import os
import paho.mqtt.client as mqtt
import mysql.connector
from mysql.connector import errorcode


#-----------------------------------------------------------------------------------
# The callback for when the mqtt client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
  if options.debug:
    print "this is on_connect. Connection result: "+mqtt.connack_string(rc)
  # Subscribing in on_connect() means that if we lose the connection and
  # reconnect then subscriptions will be renewed.
  client.subscribe("status/#")

# msg
# an instance of MQTTMessage. This is a class with members topic, payload, qos, retain.    

#-----------------------------------------------------------------------------------
##
#  persist/set/<devid>/<var> <value>
##
def on_message_status(mosq, obj, msg):
  # This callback will only be called for messages with topics that match
  # persist/set/#
  spayload = msg.payload.decode()
  stopic = msg.topic.decode()
  if options.debug:
    print "this is on_message_status. t: "+stopic+" q: "+str(msg.qos)+" p: "+spayload
    
  try:
    cnx = mysql.connector.connect(user=creds["mysql"][1]["user"],password=creds["mysql"][1]["password"],
                  database='IOT_Devices', charset="utf8mb4", collation="utf8mb4_general_ci", use_unicode=True)
  except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
      print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
      print("Database does not exist")
    else:
      print(err)

  cursor = cnx.cursor()

  # First determine which subscription has kicked in ;)
  # ~ stopic = str(msg.topic)
  last_slash = stopic.rindex("/");
  search_devid = stopic[last_slash+1:]
  # ~ spayload = str(msg.payload)
  # if ":" in spayload:
    # equals = spayload.index(":") 
    # elif
  if "=" in spayload:
    equals = spayload.index("=")
  else:
    print("Need a separator =")
    exit(2)
  varname = spayload[0:equals]
  varname = varname.replace('-','[',1)
  varname = varname.replace('-',']',1)
  varvalue = spayload[equals+1:]
  if options.debug:
    print "Varname = '"+varname+"', varvalue = '"+varvalue+"'"
  query = ("SELECT id FROM devices WHERE name = %s")
  params = [search_devid]
  if options.debug:
    print "executing query '"+query+"'",params
  cursor.execute(query,params)

  # loop over found variables
  row = cursor.fetchone()
  if row is not None:
    row_id = row[0];
    if options.debug:
      print "found id = '"+str(row_id)+"'"
    cursor.close()
    cursor = cnx.cursor()
    query = ("UPDATE `devices` SET `" + varname + "` = '" + varvalue + "' WHERE id = " + str(row_id))
  else:
    query = ("INSERT INTO `devices` (`name`, `"+varname+"`) VALUES ('"+search_devid+"', '"+varvalue+"')")
  if options.debug:
    print "Executing query '"+query+"'"
  cursor.execute(query)
  cnx.commit()
  cursor.close()
  cnx.close()

#-----------------------------------------------------------------------------------
##
# persist/fetch <devid>
##    
def on_message_fetch(mosq, obj, msg):
  # This callback will only be called for messages with topics that match
  # persist/fetch
  if options.debug:
    print "this is on_message_fetch. t: "+str(msg.topic)+" p: "+str(msg.payload)+" qos: "+str(msg.qos)
  
  # search vars table in database
  # Connect to mysql on local host
  try:
    cnx = mysql.connector.connect(user=creds["mysql"][1]["user"],password=creds["mysql"][1]["password"],
                                  database='mosquitto_fleet')
  except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
      print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
      print("Database does not exist")
    else:
      print(err)

  cursor = cnx.cursor()

  # First determine which subscription has kicked in ;)
  search_devid = str(msg.payload)
  query = ("SELECT varname, varvalue FROM vars WHERE devid = '" + search_devid + "'")
  cursor.execute(query)
  #print( "executed query" )

  # loop over found variables
  row = cursor.fetchone()
  while row is not None:
    #row = rows[0]
    if options.debug:
      print( "pub: persist/"+search_devid+"/set "+row[0]+":"+row[1] )
    client.publish( "persist/"+search_devid+"/set", row[0]+":"+row[1] )
    row = cursor.fetchone()

  cursor.close()
  cnx.close()

#-----------------------------------------------------------------------------------
# The callback for when a(n unexpected) PUBLISH message is received from the server.
def on_message(client, userdata, msg):
  print("BYTES: "+msg.topic+" "+str(msg.qos)+" "+str(msg.payload))

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
thispath = os.path.realpath(__file__).rsplit("/",1)[0]
with open(thispath+'/credentials', 'r') as file:
  jcreds = file.read().replace('\n','')
  file.close()
  
creds = json.loads(jcreds)
if (options.debug):
  print ("mqttuser = ["+creds["mqtt"]["user"]+"], password = ["+creds["mqtt"]["password"]+"]")
  print ("mysqluser2 = ["+creds["mysql"][1]["user"]+"], password = ["+creds["mysql"][1]["password"]+"]")

client = mqtt.Client()
client.on_connect = on_connect
# Add message callbacks that will only trigger on a specific subscription match.
client.message_callback_add("status/#", on_message_status)
client.on_message = on_message
client.username_pw_set(creds["mqtt"]["user"],creds["mqtt"]["password"])
client.connect("localhost", 1883, 60)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()

