#!/usr/bin/python
import datetime
import paho.mqtt.client as mqtt
import mysql.connector
from mysql.connector import errorcode


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, rc):
    #print("Connected with result code "+str(rc))
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("sensors/#")

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    # search pseudobind table in database
    # Connect to mysql on local host
    try:
      cnx = mysql.connector.connect(user='pseudobind',password='mqtt~2015',
                                    database='mosquitto_fleet')
    except mysql.connector.Error as err:
      if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
      elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
      else:
        print(err)

    cursor = cnx.cursor()

    # query = ("SELECT COUNT(*) FROM datalogger")
    # cursor.execute(query)
    # rowcounta = cursor.fetchone()
    # if rowcounta is not None:
      # rowcount = rowcounta[0]
      # while rowcount > 20:
        # # Delete first row
        # delete_stmt = "DELETE FROM datalogger ORDER BY id LIMIT 1"
        # cursor.execute(delete_stmt)
        # cnx.commit()
        
# msg.topic - split by /
# msg.payload
    #print(msg.topic+" -> "+msg.payload)
    splittopic = [x.strip() for x in msg.topic.split("/")]
    insert_stmt = "INSERT INTO datalogger (devid,sensorid,value,units) VALUES ('"+splittopic[1]+"','"+splittopic[2]+"','"+msg.payload+"','"+splittopic[3]+"')"
    #"INSERT Ime.datetime.now())+"','"+splittopic[0]+"','"+splittopic[1]+"','"+msg.payload+"','"+splittopic[2]+"')"
    #print(insert_stmt)
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


client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.username_pw_set("ESP8266","I am your father")
client.connect("localhost", 1883, 60)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()
