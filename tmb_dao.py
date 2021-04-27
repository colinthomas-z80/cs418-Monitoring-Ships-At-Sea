import json
import mysql.connector
import random
from mysql.connector import errorcode
from mysqlutils import SQL_runner

ex = '{"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":244265000,"MsgType":"position_report", \
     "Position":{"type":"Point","coordinates":[55.522592,15.068637]},"Status":"Under way using engine","RoT":2.2,"SoG":14.8,"CoG":62,"Heading":61}'

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
            ais_query = "INSERT INTO TABLE AIS_MESSAGE VALUES ({0}, {1}, {2}, {3}, {4})".format(
                    id, timestamp, mmsi, msgclass, "NULL")
            pos_quert = "INSERT INTO TABLE POSITION_REPORT VALUES({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}".format(
                    id, status, longitude, latitude, rot, sog, cog, heading, NULL, 1, NULL, NULL)
            
            

        elif msgtype == "static_data":
            imo, callsign, name, vesseltype, cargotype, length, breadth, draught, destination,
            eta, a, b, c, d = static_extract(msg)


        
        print(status)


def pre_extract(data):
    return data["Timestamp"], data["Class"], data["MMSI"], data["MsgType"]

def pos_extract(data):
    return data["Position"], data["Status"], data["RoT"], data["SoG"], data["CoG"], data["Heading"]

def static_extract(data):
    return (data["IMO"], data["CallSign"], data["Name"], data["VesselType"], data["CargoType"], data["Length"], data["Breadth"],
           data["Draught"], data["Destination"], data["ETA"], data["A"], data["B"], data["C"], data["D"])


dao = tmb_dao()

dao.insert_msg(ex)
