#!/usr/bin/python
import datetime
import optparse
import argparse
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
  client.subscribe("sensors/#")

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
  if options.debug:
    print "this is on_message. t: "+str(msg.topic)+" p: "+str(msg.payload)
  # search pseudobind table in database
  # Connect to mysql on local host
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
      
  if options.debug:
    print(msg.topic+" -> "+msg.payload)
  # ~ splittopic = [x.strip() for x in msg.topic.split("/")]
  (dummy,devname,sensorid,units) = (x.strip() for x in msg.topic.split("/"))
  if options.debug:
    print ("devname: ",devname," sensorid: ",sensorid," units: ",units)
  # ~ id 	device_id 	time_stamp 	sensorid 	units 	value 
  qry = "SELECT id FROM devices WHERE name = %s"
  prm = [devname]
  if options.debug:
    print (qry,prm)
  cursor.execute( qry, prm)
  row = cursor.fetchone()
  if row is not None:
    if options.debug:
      print ("devid: ",row[0])
    devid = row[0]
    
  count_stmt = "SELECT COUNT(id) FROM sensorlog"
  cursor.execute(count_stmt)  
  row = cursor.fetchone()
  if row is not None:
    if options.debug:
      print ("log_count: ",row[0])
    log_count = row[0]
  if log_count > options.maxrecords:
    # Delete 1000 oldest records
    if options.debug:
      print ("Deleting 1000 records") 
    del_stmt = "DELETE FROM sensorlog ORDER BY id LIMIT 1000"
    cursor.execute(del_stmt)
    cnx.commit()

  # ~ Log the sensor data
  insert_stmt = "INSERT INTO sensorlog (device_id,sensorname,value,units) VALUES (%s,%s,%s,%s)"
  insert_vals = (devid, sensorid, msg.payload, units)
  
  if options.debug:
    print(insert_stmt, insert_vals)
  cursor.execute( insert_stmt, insert_vals )
  cnx.commit()

  ### Insert / update latest reading table
  search_stmt = "SELECT * FROM sensorlatest WHERE (device_id,sensorname) = (%s,%s)"
  search_parm = (devid,sensorid)
  if options.debug:
    print(search_stmt, search_parm)
  cursor.execute(search_stmt, search_parm)

  # We expect only a single result as this is a unique field (cid)
  row = cursor.fetchone()
  if row is None:
    insert_stmt = "INSERT INTO sensorlatest (device_id,sensorname,value,units) VALUES (%s,%s,%s,%s)"
    insert_vals = (devid, sensorid, msg.payload, units)
    if options.debug:
      print(insert_stmt, insert_vals)
    cursor.execute( insert_stmt, insert_vals )
    cnx.commit()

  else:
    sql_stmt = "UPDATE sensorlatest SET value = %s WHERE (devid,sensorname) = (%s,%s)"
    sql_vals = (msg.payload, devid, sensorid)
    if options.debug:
      print(sql_stmt, sql_vals)
    cursor.execute(sql_stmt,sql_vals)
    cnx.commit()
  
  cursor.close()
  cnx.close()


#-----------------------------------------------------------------------------------
# MAIN
#-----------------------------------------------------------------------------------
## Get command line option(s)
parser = argparse.ArgumentParser(description='Log incoming MQTT sensor readings to MySQL database.')
parser.add_argument('-d','--debug',
                  dest="debug",
                  default=False,
                  action="store_true",
                  )
parser.add_argument('-m','--maxrecords',
                  dest="maxrecords",
                  nargs=1,
                  default=10000000,
                  type=int,
                  # ~ action="store_const",
                  )
(options) = parser.parse_args()

## Get credentials from json formatted file 'credentials'
thispath = os.path.realpath(__file__).rsplit("/",1)[0]
with open(thispath+'/credentials', 'r') as file:
  jcreds = file.read().replace('\n','')
  file.close()
  
creds = json.loads(jcreds)
if options.debug:
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
