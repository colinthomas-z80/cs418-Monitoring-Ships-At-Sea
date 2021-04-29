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

    # takes individual ais message as a string or json object
    def insert_msg(self, ais_json, is_json):

        if is_json == 0: # message is a string
            msg = json.loads(ais_json)
            timestamp, msgclass, mmsi, msgtype = pre_extract(msg)
        else:            # message is already json
            msg = ais_json
            timestamp, msgclass, mmsi, msgtype = pre_extract(msg)

        if msgtype == "position_report":
            position_obj, status, rot, sog, cog, heading = pos_extract(msg)
            longitude = msg["Position"]["coordinates"][0]
            latitude  = msg["Position"]["coordinates"][1]

            id = random.randint(0,4294967290)
            ais_query = "INSERT INTO AISDraft.AIS_MESSAGE VALUES ({0}, STR_TO_DATE('{1}','%Y-%m-%dT%H:%i:%s.000Z'), {2}, '{3}', {4});".format(
                    id, timestamp, mmsi, msgclass, "NULL")
            pos_query = "INSERT INTO AISDraft.POSITION_REPORT VALUES({0}, '{1}', {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11});".format( 
                    id, status, longitude, latitude, rot, sog, cog, heading, "NULL", "NULL", "NULL", "NULL") 

            try:
                SQL_runner().run(ais_query)
                SQL_runner().run(pos_query)
            except mysql.connector.Error:
                pass

        elif msgtype == "static_data":
            imo, callsign, name, vesseltype, cargotype, length, breadth, draught, destination, eta, destinationport = static_extract(msg)


            id = random.randint(0,4294967290)
            ais_query = "INSERT INTO AISDraft.AIS_MESSAGE VALUES ({0}, STR_TO_DATE('{1}','%Y-%m-%dT%H:%i:%s.000Z'),{2},'{3}',{4});".format(
                    id, timestamp, mmsi, msgclass, "NULL")
            static_query = ("INSERT INTO AISDraft.STATIC_DATA VALUES ({0},{1},'{2}','{3}','{4}','{5}',{6},{7},{8},'{9}',\
                STR_TO_DATE('{10}','%Y-%m-%dT%H:%i:%s.000Z'),{11});".format(
                id, imo, callsign, name, vesseltype, cargotype, length, breadth, draught, destination, eta, destinationport))
            try:
                SQL_runner().run(ais_query)
                SQL_runner().run(static_query)
            except mysql.connector.Error:
                pass




    # takes file of json ais messages
    def insert_message_batch(self, batch_ais_json):
        f = open(batch_ais_json)
        batch_as_json = json.loads(f.read())
        for msg in batch_as_json:
            self.insert_msg(msg, 1)
        f.close()


def pre_extract(data):
    return data["Timestamp"], data["Class"], data["MMSI"], data["MsgType"]

def pos_extract(data):
    rot = data["RoT"] if "RoT" in data else "NULL"
    sog = data["SoG"] if "SoG" in data else "NULL"
    cog = data["CoG"] if "CoG" in data else "NULL"
    position = data["Position"] if "Position" in data else "NULL"
    status = data["Status"] if "Status" in data else "NULL"
    heading = data["Heading"] if "Heading" in data else "NULL"
    return position, status, rot, sog, cog, heading

def static_extract(data):
    #special null checking intentional accounting for typo
    try:   
        cargotype = data["CargoType"]
    except KeyError:
        cargotype = data["CargoTye"] if "CargoTye" in data else "NULL"

    callsign = data["CallSign"] if "CallSign" in data else "NULL"
    name = data["Name"] if "Name" in data else "NULL"
    imo = data["IMO"] if data["IMO"] != "Unknown" else "NULL"
    vesseltype = data["VesselType"] if "VesselType" in data else "NULL"
    length = data["Length"] if "Length" in data else "NULL"
    breadth = data["Breadth"] if "Breadth" in data else "NULL"
    draught = data["Draught"] if "Draught" in data else "NULL"
    destination = data["Destination"] if "Destination" in data else "NULL"
    eta = data["ETA"] if "ETA" in data else "NULL"
    destinationport = data["DestinationPort"] if "DestinationPort" in data else "NULL"
    return (imo, callsign, name, vesseltype, cargotype, length, breadth,
           draught, destination, eta, destinationport)


#tmb_dao().insert_message_batch("sample_input.json")

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
        self.rot = msg['RoT'] if 'RoT' in msg else 'NULL',      # these commas might be unintentional?
        self.sog = msg['SoG'] if 'SoG' in msg else 'NULL',
        self.cog = msg['CoG'] if 'CoG' in msg else 'NULL',
        self.heading = msg['Heading'] if 'Heading' in msg else 'NULL'

    def to_position_report_sql_values(self):

        if not self.id:
            return None

