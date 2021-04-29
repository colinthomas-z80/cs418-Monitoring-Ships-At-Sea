import tmb_dao
from mysql.connector import errorcode
from mysqlutils import SQL_runner

class tmb_extended:

    # given an mmsi, go through the most recent AIS_MESSAGEs until you find a position report. return the mmsi and position.
    def read_position_by_mmsi(self, mmsi):
        query = "SELECT MMSI, Latitude, Longitude FROM AISDraft.POSITION_REPORT, AISDraft.AIS_MESSAGE \
                  WHERE POSITION_REPORT.AISMessage_Id = AIS_MESSAGE.Id  \
                  AND AIS_MESSAGE.MMSI = {0} \
                  ORDER BY Timestamp \
                  LIMIT 1;".format(mmsi) 
                  
        result = SQL_runner().run(query)
        return result
    
    # Find all mmsi with a position report on file. return result set of mmsi and position
    # 98000 vessels have values for mmsi, where 105894 do not. I will have to ask if that is okay.
    def read_recent_vessel_positions(self):
        runnr = SQL_runner()
        query2 = "\
                  SELECT MMSI, MAX(Timestamp) FROM AISDraft.POSITION_REPORT, AISDraft.AIS_MESSAGE \
                  WHERE POSITION_REPORT.AISMessage_Id = AIS_MESSAGE.Id GROUP BY MMSI;"
                  
        result = runnr.run(query2)
        return result

print(tmb_extended().read_recent_vessel_positions())
#print(tmb_extended().read_position_by_mmsi(235095435))