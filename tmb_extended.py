import tmb_dao
from mysql.connector import errorcode
from mysqlutils import SQL_runner
import datetime

class tmb_extended:

    # given an mmsi, go through the most recent AIS_MESSAGEs until you find a position report. return the mmsi and position.
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

    # find position reports logged inside the tile id. discard if there is a newer document for that mmsi
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


#print(tmb_extended().read_position_by_mmsi(244089000)) 

#print(tmb_extended().read_last_5_positions(244089000))

#print(tmb_extended().read_recent_vessel_positions())

#print(tmb_extended().read_port_by_name("Jyllinge"))

#print(tmb_extended().read_positions_of_ships_headed_to_port("HANSTHOLM"))

#print(tmb_extended().read_positions_of_ships_headed_to_port_id(2974))

#print(tmb_extended().delete_ais_older_than_five_minutes("2021-11-18 00:00:03"))

#print(tmb_dao.map_location(57.077635, 8.203543))

#print(tmb_extended().read_ship_positions_in_tile(5237))