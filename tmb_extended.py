import tmb_dao
from mysql.connector import errorcode
from mysqlutils import SQL_runner

class tmb_extended:

    # given an mmsi, go through the most recent AIS_MESSAGEs until you find a position report. Get the position.
    def read_position_by_mmsi(self, mmsi):
        query2 = "SELECT MMSI, Latitude, Longitude FROM AISDraft.POSITION_REPORT, AISDraft.AIS_MESSAGE \
                  WHERE POSITION_REPORT.AISMessage_Id = AIS_MESSAGE.Id  \
                  AND AIS_MESSAGE.MMSI = {0} \
                  ORDER BY Timestamp \
                  LIMIT 1;".format(mmsi) 
                  
        result = SQL_runner().run(query2)
        return result
    

tmb_extended().read_position_by_mmsi(992111929)