import json
import mysql.connector
import random
from mysql.connector import errorcode
from mysqlutils import SQL_runner

ex = '{"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":244265000,"MsgType":"position_report", \
     "Position":{"type":"Point","coordinates":[55.522592,15.068637]},"Status":"Under way using engine","RoT":2.2,"SoG":14.8,"CoG":62,"Heading":61}'

ex2 = '{"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":219023635,"MsgType":"static_data","IMO":"Unknown","CallSign":"OX3103",\
    "Name":"SKJOLD R","VesselType":"Other","CargoTye":"No additional information","Length":12,"Breadth":4,"Draught":1.5,"Destination":"HANSTHOLM",\
        "ETA":"2021-07-14T23:00:00.000Z","A":8,"B":4,"C":2,"D":2}'

class tmb_dao:

    # individual json object
    def insert_msg(self, ais_json):
        msg = json.loads(ais_json)
        timestamp, msgclass, mmsi, msgtype = pre_extract(msg)

        if msgtype == "position_report":
            position_obj, status, rot, sog, cog, heading = pos_extract(msg)
            longitude = msg["Position"]["coordinates"][0]
            latitude  = msg["Position"]["coordinates"][1]

            id = random.randint(1, 16777214)
            ais_query = "INSERT INTO AISDraft.AIS_MESSAGE VALUES ({0}, STR_TO_DATE('{1}','%Y-%m-%dT%H:%i:%s.000Z'), {2}, '{3}', {4});".format(
                    id, timestamp, mmsi, msgclass, "NULL")
            pos_query = "INSERT INTO AISDraft.POSITION_REPORT VALUES({0}, '{1}', {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11});".format( 
                    id, status, longitude, latitude, rot, sog, cog, heading, "NULL", "NULL", "NULL", "NULL") 

            print(SQL_runner().run(ais_query))
            print(SQL_runner().run(pos_query))
            

        elif msgtype == "static_data":
            imo, callsign, name, vesseltype, cargotype, length, breadth, draught, destination, eta, a, b, c, d = static_extract(msg)


            id = random.randint(1, 16777214)
            ais_query = "INSERT INTO AISDraft.AIS_MESSAGE VALUES ({0}, STR_TO_DATE('{1}','%Y-%m-%dT%H:%i:%s.000Z'),{2},'{3}',{4});".format(
                    id, timestamp, mmsi, msgclass, "NULL")

            static_query = ("INSERT INTO AISDraft.STATIC_DATA VALUES ({0},{1},'{2}','{3}','{4}','{5}',{6},{7},{8},'{9}',\
                STR_TO_DATE('{10}','%Y-%m-%dT%H:%i:%s.000Z'),{11},{12},{13},{14});".format(
                id, imo, callsign, name, vesseltype, cargotype, length, breadth, draught, destination, eta, a, b, c, d))
            
            print(SQL_runner().run(ais_query))
            print(SQL_runner().run(static_query))
            

def pre_extract(data):
    return data["Timestamp"], data["Class"], data["MMSI"], data["MsgType"]

def pos_extract(data):
    return data["Position"], data["Status"], data["RoT"], data["SoG"], data["CoG"], data["Heading"]

def static_extract(data):
    ct = data["CargoTye"] if "CargoTye" in data else data["CargoType"] # intentional accounting for typo
    imo = data["IMO"] if data["IMO"] != "Unknown" else "NULL"
    return (imo, data["CallSign"], data["Name"], data["VesselType"], ct, data["Length"], data["Breadth"],
           data["Draught"], data["Destination"], data["ETA"], data["A"], data["B"], data["C"], data["D"])


dao = tmb_dao()

dao.insert_msg(ex)

def insert_message_batch(self, batch):
    if type(batch) is str:
        print("Incorrect parameter type: should be a list of messages.")
        return -1
    if self.is_stub:
        return len(batch)

        cursor = con.cursor()

        inserted = 0

        for msg in batch:
            timestamp = msg['Timestap'][:-1].replace('T', ' ')

            try:
                cursor.execute("INSERT INTO AIS_MESSAGE VALUES (NULL, '{}', '{}', '{}', NULL)".format(timestamp, msg['MMSI'], msg['Class']))

                last_id = cursor.lastrowid

                if msg['MsgType'] == 'position_report':
                    query = "INSERT INTO POSITION_REPORT VALUES ({}, '{}', {}, {}, {}, {}, {},{}, NULL, NULL, NULL, NULL)".format(
                        last_id, msg['Status'],
                        msg['Position']['Coordinates'][1],
                        msg['Position']['Coordinates'][0],
                        msg['RoT'] if 'RoT' in msg else 'NULL',
                        msg['SoG'] if 'SoG' in msg else 'NULL',
                        msg['CoG'] if 'CoG' in msg else 'NULL',
                        msg['Heading'] if 'Heading' in msg else 'NULL',)
                cursor.execute(query)
                print(SQL_runner().run(query))
                print(f"INSERTED: {cursor.rowcount}")

            except Exception as e:
                print(e)

        return inserted

# Renet showed this code in class so if this helps in any way
class Message:
    def __init__(self, msg):

        self.timestamp = msg['Timestamp'][:-1].replace('T', ' ')
        self.mmsi = msg['MMSI']
        self.equiptclass = msg['Class']

    def to_shared_sql_values(self):
        return "(NULL, '{}', {}, '{}', NULL)".format(self.timestamp, self.mmsi, self.equiptclass)


class PositionReport:

    def __init__(self, msg):

        super().__init__(msg)

        self.id = None
        self.status = msg['Status']
        self.longitude = msg['Position']['coordinates'][1]
        self.latitude = msg['Position']['coordinates'][0]
        self.rot = msg['RoT'] if 'RoT' in msg else 'NULL',
        self.sog = msg['SoG'] if 'SoG' in msg else 'NULL',
        self.cog = msg['CoG'] if 'CoG' in msg else 'NULL',
        self.heading = msg['Heading'] if 'Heading' in msg else 'NULL'

    def to_position_report_sql_values(self):

        if not self.id:
            return None

