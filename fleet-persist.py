#!/usr/bin/python
import datetime
import optparse
import os
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
  search_devname = str(msg.topic)[last_slash+1:]
  spayload = str(msg.payload)
  # if ":" in spayload:
    # colon = spayload.index(":") 
    # elif
  if "=" in spayload:
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
  query = ("SELECT varname, varvalue FROM vars WHERE devid = '" + search_devname + "' AND varname = '" + varname + "'")
  if options.debug:
    print "executing query '"+query+"'"
  cursor.execute(query)

  # loop over found variables
  row = cursor.fetchone()
  if row is not None:
    cursor.close()
    cursor = cnx.cursor()
    query = ("UPDATE vars SET varvalue = '" + varvalue + "' WHERE devid = '" + search_devname + "' AND varname = '" + varname +"'")
  else:
    query = ("INSERT INTO `vars` (`id`, `devid`, `varname`, `varvalue`) VALUES (NULL, '"+search_devname+"', '"+varname+"', '"+varvalue+"')")
  
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
  search_devname = msg.payload.decode()
  device_id = "" # Set scope to be whole function
  if options.debug:
    print "this is on_message_fetch. t: "+str(msg.topic)+" p: "+search_devname+" qos: "+str(msg.qos)
  
  # search vars table in database
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

  # First determine which subscription has kicked in ;)
  # ~ query = ("SELECT varname, varvalue FROM vars WHERE devid = '" + search_devname + "'")
  query = ("SELECT variables.varname, variables.varvalue, devices.id "+
          "FROM variables "+
          "INNER JOIN devices ON variables.device = devices.id "+
          "WHERE devices.name = %s")
  prm = [search_devname]
  cursor.execute(query,prm)
  #print( "executed query" )

  # loop over found variables
  row = cursor.fetchone()
  while row is not None:
    device_id = row[2]
    #row = rows[0]
    if options.debug:
      print( "pub: persist/"+search_devname+"/set "+row[0]+":"+row[1] )
    client.publish( "persist/"+search_devname+"/set", row[0]+":"+row[1] )
    row = cursor.fetchone()

# ~ valvecount int
# ~ valvepins[0-9] int,int
# ~ valvestate[0-9] int
# ~ sensorcount int
# ~ sensorpins[0-9] string(int[[,int]...])
# ~ sensortype[0-9] int
  ############################################
  # Return sensor data as variables
  ############################################
  qry = ("SELECT sensors.id,sensor_types.deprecated_id FROM sensors "+
          "INNER JOIN sensor_types ON sensor_types.id = sensors.type "+
          "WHERE sensors.attached_to = %s")
  prm = [device_id]
  cursor.execute( qry, prm )
  sensors = cursor.fetchall()
  if options.debug:
    print("Total rows are:  ", len(sensors))
    print("Printing each row")
  sensor_index = -1
  sensor_dtype = []
  pinstring = []
  for row in sensors:
    sensor_id = row[0]
    sensor_index += 1
    sensor_dtype.append(row[1])
    pinstring.append("")
    if options.debug:
      print("sensors.id: ", sensor_id,"sensor_dtype: ",row[1])
    qry = ("SELECT device_characteristics.pin_name,device_characteristics.pin_gpio "+
            "FROM sensor_pins "+
            "INNER JOIN device_characteristics "+
            "ON device_characteristics.id = sensor_pins.device_characteristics_id "+
            "WHERE sensor_pins.sensor = %s")
    prm = [sensor_id]
    cursor.execute( qry, prm )
    pinrows = cursor.fetchall()
    if options.debug:
      print("Total sensor pins are:  ", len(pinrows))
    for pin in pinrows:
      pinstring[sensor_index] = pinstring[sensor_index] + "," + str(pin[1])
      if options.debug:
        print("Pin_name: ", pin[0], " GPIO: ", pin[1])
    pinstring[sensor_index] = pinstring[sensor_index][1:]
    if options.debug:
      print("pinstring[",sensor_index,"] = `",pinstring[sensor_index],"`")
      
       
    if options.debug:
      print( "pub: persist/"+search_devname+"/set","sensorpins["+str(sensor_index)+"]:"+pinstring[sensor_index] )
    client.publish( "persist/"+search_devname+"/set","sensorpins["+str(sensor_index)+"]:"+pinstring[sensor_index] )
    if options.debug:
      print( "pub: persist/"+search_devname+"/set","sensortype["+str(sensor_index)+"]:"+sensor_dtype[sensor_index] )
    client.publish( "persist/"+search_devname+"/set","sensortype["+str(sensor_index)+"]:"+sensor_dtype[sensor_index] )
    
  if options.debug:
    print( "pub: persist/"+search_devname+"/set", "sensorcount:"+str(sensor_index+1) )
  client.publish( "persist/"+search_devname+"/set", "sensorcount:"+str(sensor_index+1) )
  # ~ client.publish( "persist/"+search_devname+"/set", row[0]+":"+row[1] )

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

