import json
import datetime
import mysql.connector
import random
from mysql.connector import errorcode
from mysqlutils import SQL_runner

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
            
            id = random.randint(0,4294967290)
            longitude = msg["Position"]["coordinates"][1]
            latitude  = msg["Position"]["coordinates"][0]

            # find out which map tiles contain these coordinates
            mv1, mv2, mv3 = map_location(latitude, longitude)

            ais_query = "INSERT INTO AISDraft.AIS_MESSAGE VALUES ({0}, STR_TO_DATE('{1}','%Y-%m-%dT%H:%i:%s.000Z'), {2}, '{3}', {4});".format(
                    id, timestamp, mmsi, msgclass, "NULL")
            pos_query = "INSERT INTO AISDraft.POSITION_REPORT VALUES({0}, '{1}', {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11});".format( 
                    id, status, longitude, latitude, rot, sog, cog, heading, "NULL", mv1, mv2, mv3) 

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


    def read_position_by_mmsi(self, mmsi):
        query = "SELECT MMSI, Latitude, Longitude, max(Timestamp) FROM AISDraft.POSITION_REPORT, AISDraft.AIS_MESSAGE \
                  WHERE POSITION_REPORT.AISMessage_Id = AIS_MESSAGE.Id  \
                  AND AIS_MESSAGE.MMSI = {0} \
                  GROUP BY MMSI, Latitude, Longitude".format(mmsi) 
                  
        result = SQL_runner().run(query)
        return result

    def read_last_5_positions(self, mmsi):
        query = "SELECT MMSI, Latitude, Longitude FROM AISDraft.POSITION_REPORT, AISDraft.AIS_MESSAGE \
                  WHERE POSITION_REPORT.AISMessage_Id = AIS_MESSAGE.Id  \
                  AND AIS_MESSAGE.MMSI = {0} \
                  ORDER BY Timestamp \
                  LIMIT 5;".format(mmsi) 
                  
        result = SQL_runner().run(query)
        return result
    
    # Find all mmsi with a position report on file. return result set of mmsi and position
    # 98000 vessels have values for mmsi, where 105894 do not. I will have to ask if that is okay.
    def read_recent_vessel_positions(self):
        runnr = SQL_runner()
        # find unique mmsi that have a position report
        query2 = "\
                  SELECT DISTINCT MMSI FROM AISDraft.POSITION_REPORT, AISDraft.AIS_MESSAGE \
                  WHERE POSITION_REPORT.AISMessage_Id = AIS_MESSAGE.Id GROUP BY MMSI;"
        result = runnr.run(query2)

        positions = []
        for mmsi in result:
            rs = self.read_position_by_mmsi(mmsi[0])
            positions.append(rs)

        return positions

    # Return the vessel row that matches the given mmsi
    def read_vessel_information(self, mmsi):
        query = "SELECT * FROM AISDraft.VESSEL WHERE MMSI = {0}".format(mmsi)
        rs = SQL_runner().run(query)
        return rs

    # return a port with given name
    def read_port_by_name(self, name):
        query = "SELECT * FROM AISDraft.PORT WHERE Name = '{0}'".format(name)
        rs = SQL_runner().run(query)
        return rs

    # go through static data to find all instances where the port is the destination
    # find the AIS_MESSAGE parent of the static data, get the distinct mmsi from each vessel
    # then get the most recent position report from each vessel
    #
    # if there are no values returned, return an array of port documents, per the requirements. 
    def read_positions_of_ships_headed_to_port(self, name):
        dest = "SELECT AISMessage_Id FROM AISDraft.STATIC_DATA WHERE AISDestination = '{0}'".format(name)
        query = "SELECT DISTINCT MMSI FROM AISDraft.AIS_MESSAGE WHERE Id IN ({0})".format(dest)
        mmsi_set = SQL_runner().run(query)
        
        positions = []
        for mmsi in mmsi_set:
            rs = self.read_position_by_mmsi(mmsi[0])
            positions.append(rs)
        
        if not positions[0]:
            ports = SQL_runner().run("SELECT * FROM AISDraft.PORT;")
            return ports
        return positions


    # basically the same as the last query but you need to find the name of the port by its id first
    # is the port id the number id or the LoCode? I don't know. Right now I figure it is the number.
    def read_positions_of_ships_headed_to_port_id(self, id):
        get_name = "SELECT NAME FROM AISDraft.PORT WHERE Id = '{0}'".format(id)
        name = SQL_runner().run(get_name)
        
        if not name: # there is no port with that id
            return []
        else:
            dest = "SELECT AISMessage_Id FROM AISDraft.STATIC_DATA WHERE AISDestination = '{0}'".format(name[0][0])
            query = "SELECT DISTINCT MMSI FROM AISDraft.AIS_MESSAGE WHERE Id IN ({0})".format(dest)
            mmsi_set = SQL_runner().run(query)
            
            positions = []
            for mmsi in mmsi_set:
                rs = self.read_position_by_mmsi(mmsi[0])
                positions.append(rs)
            
            if not positions[0]:
                ports = SQL_runner().run("SELECT * FROM AISDraft.PORT;")
                return ports
            return positions

    # delete messages older than 5 minutes, from the time passed to the function. That should
    # make it easier to test. Pass the date as a string in the mysql format of "YYYY-MM-DD HH:MM:SS"
    def delete_ais_older_than_five_minutes(self, current_time):
        now = datetime.datetime.strptime(current_time, '%Y-%m-%d %H:%M:%S')
        before = now - datetime.timedelta(0,300)

        query = "DELETE FROM AISDraft.AIS_MESSAGE WHERE Timestamp < '{0}'".format(before) # deletes null values too, sounds good to me
        deleted = SQL_runner().run(query)  
        return deleted

    # find mmsis from position reports logged inside the tile id, then get the ship information for each mmsi
    def read_ship_positions_in_tile(self, id):
        query = "SELECT DISTINCT MMSI FROM AISDraft.AIS_MESSAGE  \
                 WHERE Id in (SELECT DISTINCT AISMessage_Id FROM AISDraft.POSITION_REPORT, AISDraft.AIS_MESSAGE  \
                 WHERE MapView1_Id = '{0}' OR MapView2_Id = '{0}' OR MapView3_Id = '{0}' \
                 AND AISMessage_Id = Id)".format(id)

        mmsi_set = SQL_runner().run(query)
        vessels = []
        for mmsi in mmsi_set:
            vessels.append(self.read_vessel_information(mmsi[0]))
        return vessels


    # get the level 3 tile the port is in, and read the ship positions in that tile
    def read_ship_positions_by_port(self, port_name):
        tile = SQL_runner().run("SELECT PORT.MapView3_Id FROM AISDraft.PORT WHERE Name = '{0}'".format(port_name))
        if not tile[0]:
            ports = SQL_runner().run("SELECT * FROM AISDraft.PORT;")
            return "ports"
        else:
            vessels = self.read_ship_positions_in_tile(tile[0][0])
            return vessels


    # get the 4 child tiles of a given level 2 tile id
    def read_level_3_tiles(self, id):
        tiles = SQL_runner().run("SELECT Id FROM AISDraft.MAP_VIEW WHERE ContainerMapView_Id = {0}".format(id))
        rs = []
        for tile in tiles:  # get it out of a 2 dimensional array
            rs.append(tile[0])
        return rs
    
    def get_tile_file(self, id):
        file = SQL_runner().run("SELECT RasterFile FROM AISDraft.MAP_VIEW WHERE Id = {0}".format(id))
        return file[0][0]     

# find the 3 levels of map tile which contain the coordinate set, hopefully... 
def map_location(latitude, longitude):
    query = "SELECT Id FROM AISDraft.MAP_VIEW WHERE \
             LongitudeW < {0} AND \
             LongitudeE > {0} AND \
             LatitudeS < {1} AND \
             LatitudeN > {1};".format(longitude,latitude)

    rs = SQL_runner().run(query)
    if len(rs) <= 1:
        return 1, "NULL", "NULL" # return null if there is no map view that contains the location. top level map view will always be 1
    return 1, rs[1][0], rs[2][0]  





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

#print(map_location(57.49587, 10.501518))

#print(tmb_dao().read_position_by_mmsi(244089000)) 

#print(tmb_dao().read_last_5_positions(244089000))

#print(tmb_dao().read_recent_vessel_positions())

#print(tmb_dao().read_port_by_name("Jyllinge"))

#print(tmb_dao().read_positions_of_ships_headed_to_port("HANSTHOLM"))

#print(tmb_dao().read_positions_of_ships_headed_to_port_id(2974))

#print(tmb_dao().delete_ais_older_than_five_minutes("2021-11-18 00:00:03"))

#print(tmb_dao().read_ship_positions_in_tile(53312))

#print(tmb_dao().read_ship_positions_by_port("Munkebo"))

#print(tmb_dao().read_level_3_tiles(5036))

#print(tmb_dao().get_tile_file(5036))

