#!/usr/bin/python
import datetime
import optparse
import os
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
  search_cid = msg.payload.decode()
  if options.debug:
    print "this is on_message. t: "+str(msg.topic)+" p: "+search_cid
  # search pseudobind table in database
  # Connect to mysql on local host
  try:
    cnx = mysql.connector.connect(host='localhost',user=creds["mysql"][0]["user"],password=creds["mysql"][0]["password"],
                    database='IOT_Devices', charset="utf8mb4", collation="utf8mb4_general_ci", use_unicode=True)
  except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
      print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
      print("Database does not exist")
    else:
      print(err)
  # ~ finally:
    # ~ cursor.close()
    # ~ cnx.close()
      
  if options.debug:
    print("cnx.charset = ["+cnx.charset+"]")
    
  cursor = cnx.cursor()
  query = u"""SELECT * FROM devices WHERE cid = %s"""
# ~ qry = u"""SELECT * FROM devices WHERE cid = %s"""
  if options.debug:
    print("About to construct parameter from "+search_cid)
  # ~ prm = encode(search_cid,'utf8')
  prm = [search_cid]
  # ~ prm = [u""+search_cid]
  if options.debug:
    print("cursor.execute("+query+", "+prm[0]+")")
  cursor.execute(query,prm)  
  # ~ cursor.execute('SELECT * FROM devices')
  # ~ if options.debug:
    # ~ print("About to fetchone")
    # We expect only a single result as this is a unique field (cid)
  row = cursor.fetchone()
  # ~ if options.debug:
    # ~ print "fetched, row = "+row
    
  if row is not None:
    if options.debug:
      print ("row is not None")
      print ("row[]: ", row) 
    found_devid = row[2]
    if options.debug:
      print "found_devid: "+found_devid
    #print ("I found device ID [" + found_devid + "]")
    update_stmt = "UPDATE devices SET last_seen = '"+ str(datetime.datetime.now()) +"' WHERE cid = '" + search_cid + "'"
    if options.debug:
      print (update_stmt)
    cursor.execute(update_stmt)
    cnx.commit()
  else:
    if options.debug:
      print ("row is None")
    #cursor.fetchall() # clear the cursor for the insert statement
    found_devid = "disco-"+search_cid
    insert_stmt = "INSERT INTO devices (cid, name, last_seen) VALUES ('" + search_cid + "','" + found_devid + "','" + str(datetime.datetime.now()) + "')"
    if options.debug:
      print (insert_stmt)
    #data = (search_cid, found_devid, "<discovered device>", right_now )
    #data = ( right_now )
    cursor.execute(insert_stmt)
    cnx.commit()
  cursor.close()
  cnx.close()

  if options.debug:
    print("pub: bind/"+search_cid+" "+str(found_devid))
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
thispath = os.path.realpath(__file__).rsplit("/",1)[0]
with open(thispath+'/credentials', 'r') as file:
  jcreds = file.read().replace('\n','')
  file.close()
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
