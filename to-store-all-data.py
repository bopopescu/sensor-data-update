import mysql.connector
import requests
import time

mydb = mysql.connector.connect(
    host = "35.184.176.129",
    user = "boss",
    passwd = "Jirayu1001",
    database = "rawdata"
)

def get_smartbin():
	mycursor = mydb.cursor()
        response = requests.get("https://www.aismagellan.io/api/things/pull/f846cee0-4d60-11e9-810a-f990cf998f9d")
        bin_data = response.json()
	count = 0
	for i in range(len(bin_data["smartbin"])):
		key = bin_data["smartbin"].keys()[i]
		mycursor.execute("UPDATE sensors SET sensor_data = %s WHERE sensor_id = %s", (str(bin_data["smartbin"][str(key)]), str(key)))
		count = count + int(mycursor.rowcount)
		mydb.commit()
	print("SmartBin: Selected %s rows" % count)
	return

def get_flood_detector():
	mycursor = mydb.cursor()
	response = requests.get("https://www.aismagellan.io/api/things/pull/87bfe980-505e-11e9-810a-f990cf998f9d")
        flood_data = response.json()
	count = 0
        for i in range(len(flood_data)):
                key = flood_data['sensor_data']
                mycursor.execute("UPDATE sensors SET sensor_data = %s WHERE sensor_id = %s", (str(key),flood_data['sensor_id']))
		count = count + int(mycursor.rowcount)
                mydb.commit()
	print("Flood_detector: Selected %s rows" % count)
        return

def get_street_light():
        mycursor = mydb.cursor()
        response = requests.get("https://www.aismagellan.io/api/things/pull/cb0dd550-4fe4-11e9-96dd-9fb5d8a71344")
        streetlight_data = response.json()
        count = 0
        for i in range(len(streetlight_data)):
                key = streetlight_data['sensor_data']
                mycursor.execute("UPDATE sensors SET sensor_data = %s WHERE sensor_id = %s", (str(key),streetlight_data['sensor_id']))
                count = count + int(mycursor.rowcount)
                mydb.commit()
        print("Smart_streetlight: Selected %s rows" % count)
        return

def get_AQD():
        mycursor = mydb.cursor()
        response = requests.get("https://www.aismagellan.io/api/things/pull/e8375150-5396-11e9-96dd-9fb5d8a71344")
        AQD_data = response.json()
        count = 0
        for i in range(len(AQD_data)):
                key = AQD_data['sensor_data']
                mycursor.execute("UPDATE sensors SET sensor_data = %s WHERE sensor_id = %s", (str(key),AQD_data['sensor_id']))
                count = count + int(mycursor.rowcount)
                mydb.commit()
        print("AQD: Selected %s rows" % count)
        return

while True:
	get_smartbin()
	get_flood_detector()
	get_street_light()
	get_AQD()
	time.sleep(300)

