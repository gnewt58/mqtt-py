#!/usr/bin/python
import datetime
import optparse
import json
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
  client.subscribe("persist/fetch")
  client.subscribe("persist/set/#")

# msg
# an instance of MQTTMessage. This is a class with members topic, payload, qos, retain.    

#-----------------------------------------------------------------------------------
##
#  persist/set/<devid>/<var> <value>
##
def on_message_set(mosq, obj, msg):
  # This callback will only be called for messages with topics that match
  # persist/set/#
  if options.debug:
    print "this is on_message_set. t: "+str(msg.topic)+" q: "+str(msg.qos)+" p: "+str(msg.payload)
    
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
  stopic = str(msg.topic)
  last_slash = stopic.rindex("/");
  search_devid = str(msg.topic)[last_slash+1:]
  spayload = str(msg.payload)
  if ":" in spayload:
    colon = spayload.index(":")
  elif "=" in spayload:
    colon = spayload.index("=")
  else:
    print("Need a separator of either : or =")
    exit(2)
  varname = spayload[0:colon]
  varname = varname.replace('-','[',1)
  varname = varname.replace('-',']',1)
  varvalue = spayload[colon+1:]
  if options.debug:
    print "Varname = '"+varname+"', varvalue = '"+varvalue+"'"
  query = ("SELECT varname, varvalue FROM vars WHERE devid = '" + search_devid + "' AND varname = '" + varname + "'")
  if options.debug:
    print "executing query '"+query+"'"
  cursor.execute(query)

  # loop over found variables
  row = cursor.fetchone()
  if row is not None:
    cursor.close()
    cursor = cnx.cursor()
    query = ("UPDATE vars SET varvalue = '" + varvalue + "' WHERE devid = '" + search_devid + "' AND varname = '" + varname +"'")
  else:
    query = ("INSERT INTO `vars` (`id`, `devid`, `varname`, `varvalue`) VALUES (NULL, '"+search_devid+"', '"+varname+"', '"+varvalue+"')")
  
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
with open('credentials', 'r') as file:
  jcreds = file.read().replace('\n','')
creds = json.loads(jcreds)
print ("mqttuser = ["+creds["mqtt"]["user"]+"], password = ["+creds["mqtt"]["password"]+"]")
print ("mysqluser2 = ["+creds["mysql"][1]["user"]+"], password = ["+creds["mysql"][1]["password"]+"]")

client = mqtt.Client()
client.on_connect = on_connect
# Add message callbacks that will only trigger on a specific subscription match.
client.message_callback_add("persist/set/#", on_message_set)
client.message_callback_add("persist/fetch", on_message_fetch)
client.on_message = on_message
client.username_pw_set(creds["mqtt"]["user"],creds["mqtt"]["password"])
client.connect("localhost", 1883, 60)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()

