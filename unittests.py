import unittest as test
import create_database
import mysqlutils
import tmb_dao

class ais_unit_tests(test.TestCase):

    # Tests unit tester
    def test_if_true(self):
        self.assertTrue(True, "Pass")

    def test_if_false(self):
        self.assertFalse(False, "Pass")

    # Unit Tests for create_database.py
    def test_create_database(self):
        error = False
        try:
            create_database
            mysqlutils.SQL_runner().run("USE AISDraft")
            mysqlutils.SQL_runner().run("DESCRIBE AISDraft.VESSEL")
        except Exception:
            error = True
        self.assertFalse(error)

    # Unit Tests for tmb_dao.py

    def test_read_position_by_mmsi(self):
        tmb_dao.tmb_dao().read_position_by_mmsi(244089000)
        expected = "[(244089000, Decimal('57.077635'), Decimal('8.203543'), datetime.datetime(2020, 11, 18, 0, 0, 2))]"
        query = "SELECT MMSI, Latitude, Longitude, max(Timestamp) FROM AISDraft.POSITION_REPORT, AISDraft.AIS_MESSAGE " \
                "WHERE POSITION_REPORT.AISMessage_Id = AIS_MESSAGE.Id AND AIS_MESSAGE.MMSI = 244089000 GROUP BY MMSI, " \
                "Latitude, Longitude; "
        actual = mysqlutils.SQL_runner().run(query)
        self.assertEqual(expected, str(actual))

    def test_read_last_5_positions(self):
        tmb_dao.tmb_dao().read_last_5_positions(244089000)
        expected = "[(244089000, Decimal('57.077635'), Decimal('8.203543'))]"
        query = "SELECT MMSI, Latitude, Longitude FROM AISDraft.POSITION_REPORT, AISDraft.AIS_MESSAGE WHERE " \
                "POSITION_REPORT.AISMessage_Id = AIS_MESSAGE.Id AND AIS_MESSAGE.MMSI = 244089000 ORDER BY Timestamp " \
                "LIMIT 5; "
        actual = mysqlutils.SQL_runner().run(query)
        self.assertEqual(expected, str(actual))

    def test_read_recent_vessel_positions(self):
        try:
            positions = tmb_dao.tmb_dao().read_recent_vessel_positions()
        except Exception:
            positions = []
        print(positions)
        self.assertNotEqual(0, len(positions))

    def test_read_vessel_information(self):
        tmb_dao.tmb_dao().read_vessel_information(244089000)
        expected = "[(9229063, 'Netherlands', 'Thun Galaxy', 2001, None, 114, 15, 4107, 244089000, 'Chemical/Oil " \
                   "Tanker', 'Active', '17821')]"
        query = "SELECT * FROM AISDraft.VESSEL WHERE MMSI = 244089000"
        actual = mysqlutils.SQL_runner().run(query)
        self.assertEqual(expected, str(actual))

    def test_read_port_by_name(self):
        tmb_dao.tmb_dao().read_port_by_name("Jyllinge")
        expected = "[(5016, 'NULL', 'Jyllinge', 'Denmark', Decimal('12.096111'), Decimal('55.745000'), " \
                   "'www.jyllingehavn.dk', 1, 5528, 55283)]"
        query = "SELECT * FROM AISDraft.PORT WHERE Name = 'Jyllinge'"
        actual = mysqlutils.SQL_runner().run(query)
        self.assertEqual(expected, str(actual))

    def test_get_tile_file(self):
        tmb_dao.tmb_dao().get_tile_file(5036)
        expected = "[('38F7.png',)]"
        query = "SELECT RasterFile FROM AISDraft.MAP_VIEW WHERE Id = 5036"
        actual = mysqlutils.SQL_runner().run(query)
        self.assertEqual(expected, str(actual))

    def test_read_position_of_ships_headed_to_port(self):
        expected = "[[(219023635, Decimal('57.720800'), Decimal('10.594683'), datetime.datetime(2020, 11, 18, 0, 0, " \
                   "2))]]"
        actual = tmb_dao.tmb_dao().read_positions_of_ships_headed_to_port("HANSTHOLM")
        self.assertEqual(expected, str(actual))

    def test_read_position_of_ships_headed_to_port_id(self):
        expected = "[[(219023635, Decimal('57.720800'), Decimal('10.594683'), datetime.datetime(2020, 11, 18, 0, 0, " \
                   "2))]]"
        actual = str(tmb_dao.tmb_dao().read_positions_of_ships_headed_to_port_id(2974))
        self.assertEqual(expected, actual)

    def test_read_ship_positions_in_tile(self):
        expected = "[[(9608673, 'Marshall Islands', 'Ionic Hawk', 2012, None, 180, 30, 22432, 538004542, " \
                   "'Bulk Carrier', 'Active', '20820')]]"
        actual = str(tmb_dao.tmb_dao().read_ship_positions_in_tile(53312))
        self.assertEqual(expected, actual)

    def test_read_ship_positions_by_port(self):
        expected = "[[(9608673, 'Marshall Islands', 'Ionic Hawk', 2012, None, 180, 30, 22432, 538004542, " \
                   "'Bulk Carrier', 'Active', '20820')]]"
        actual = str(tmb_dao.tmb_dao().read_ship_positions_by_port("Munkebo"))
        self.assertEqual(expected, actual)

    def test_read_level_3_tiles(self):
        expected = "[50361, 50362, 50363, 50364]"
        actual = str(tmb_dao.tmb_dao().read_level_3_tiles(5036))
        self.assertEqual(expected, actual)

    def test_y_delete_ais_older_than_five_minutes(self):
        expected = "[(500,)]"
        actual = str(tmb_dao.tmb_dao().delete_ais_older_than_five_minutes("2020-11-18 00:09:00"))
        self.assertEqual(expected, actual)

    # Test for insert.msg()
    def test_insert_message_fail(self):
        error = False
        ex = '{"Timstamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":244265000,"MsgType":"POSITION_REPORT", \
                     "Position":{"type":"Point","coordinates":[55.522592,15.068637]},"Status":"Under way using engine","RoT":2.2,"SoG":14.8,"CoG":62,"Heading":61}'
        try:
            tmb_dao.tmb_dao().insert_msg(ex, 0)
        except KeyError:
            error = True
        self.assertTrue(error)

    # Test successful insert for insert_msg()
    def test_z_insert_message(self):
        ex = '{"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":244265000,"MsgType":"POSITION_REPORT", \
                             "Position":{"type":"Point","coordinates":[55.522592,15.068637]},"Status":"Under way using engine","RoT":2.2,"SoG":14.8,"CoG":62,"Heading":61}'
        tmb_dao.tmb_dao().insert_msg(ex, 0)
        expected = "[(datetime.datetime(2020, 11, 18, 0, 0), 'Under way using engine')]"
        query = "SELECT AISDraft.AIS_MESSAGE.Timestamp, AISDraft.POSITION_REPORT.NavigationalStatus FROM " \
                "AISDraft.AIS_MESSAGE, AISDraft.POSITION_REPORT WHERE AISDraft.AIS_MESSAGE.MMSI=244265000 AND " \
                "AISDraft.AIS_MESSAGE.Id=AISDraft.POSITION_REPORT.AISMessage_Id; "
        actual = mysqlutils.SQL_runner().run(query)
        self.assertEqual(expected, str(actual))

    # Test for insert_message_batch()
    # Test successful insert for insert_message_batch()
    def test_insert_message_batch(self):
        tmb_dao.tmb_dao().insert_message_batch("sample_input.json")
        true_output = "[(datetime.datetime(2020, 11, 18, 0, 0), 'Under way using engine')]"
        query = "SELECT AISDraft.AIS_MESSAGE.timestamp, AISDraft.POSITION_REPORT.NavigationalStatus FROM " \
                "AISDraft.AIS_MESSAGE, AISDraft.POSITION_REPORT WHERE AISDraft.AIS_MESSAGE.MMSI=304858000 AND " \
                "AISDraft.AIS_MESSAGE.Id=AISDraft.POSITION_REPORT.AISMessage_Id; "
        program_output = mysqlutils.SQL_runner().run(query)
        self.assertEqual(true_output, str(program_output))

if __name__ == '__main__':
    test.main()
