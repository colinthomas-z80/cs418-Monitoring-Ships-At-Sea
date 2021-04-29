import tmb_dao
from mysql.connector import errorcode
from mysqlutils import SQL_runner

class tmb_extended:

    # given an mmsi, go through the most recent AIS_MESSAGEs until you find a position report. Get the position.
    def read_position_by_mmsi(self, mmsi):
        query = ("SELECT Latitude, Longitude FROM AISDraft.POSITION_REPORT  \
                  WHERE AISMessage_Id IN                                    \
                  (SELECT Id FROM AISDraft.AIS_MESSAGE WHERE MMSI = {0});").format(mmsi)
        messages = SQL_runner().run(query)
        print(messages)
    

tmb_extended().read_position_by_mmsi(992111929)