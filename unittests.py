import unittest as test
import create_database
import mysqlutils
import tmb_dao
from mysql.connector import Error
import re


class ais_unit_tests(test.TestCase):

    # Tests unit tester
    def test_if_true(self):
        self.assertTrue(True, "Pass")

    def test_if_false(self):
        self.assertFalse(False, "Pass")

    # Unit Tests for create_database.py
    def test_create_database_pass(self):
        create_database
        error = False
        try:
            mysqlutils.SQL_runner().run("USE AISDraft")
        except Exception:
            error = True
        self.assertFalse(error)

    def test_create_database_fail(self):
        create_database
        error = False
        try:
            mysqlutils.SQL_runner().run("US AISDraft")
        except Exception:
            error = True
        self.assertTrue(error)

    # Unit Tests for tmb_dao.py

    # Test for insert.msg()
    def test_insert_message_pass(self):
        error = False
        ex = '{"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":244265000,"MsgType":"position_report", \
             "Position":{"type":"Point","coordinates":[55.522592,15.068637]},"Status":"Under way using engine","RoT":2.2,"SoG":14.8,"CoG":62,"Heading":61}'
        try:
            tmb_dao.tmb_dao().insert_msg(ex, 0)
        except Error:
            error = True
        self.assertFalse(error)

    def test_insert_message_fail(self):
        error = False
        ex = '{"Timstamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":244265000,"MsgType":"position_report", \
                     "Position":{"type":"Point","coordinates":[55.522592,15.068637]},"Status":"Under way using engine","RoT":2.2,"SoG":14.8,"CoG":62,"Heading":61}'
        try:
            tmb_dao.tmb_dao().insert_msg(ex, 0)
        except KeyError:
            error = True
        self.assertTrue(error)

    # Test successful insert for insert_msg()
    def test_insert_message(self):
        ex = '{"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":244265000,"MsgType":"position_report", \
                             "Position":{"type":"Point","coordinates":[55.522592,15.068637]},"Status":"Under way using engine","RoT":2.2,"SoG":14.8,"CoG":62,"Heading":61}'
        tmb_dao.tmb_dao().insert_msg(ex, 0)
        expected = "[(datetime.datetime(2020, 11, 18, 0, 0), 'Under way using engine')]"
        query = "SELECT AISDraft.ais_message.Timestamp, AISDraft.position_report.Navigationalstatus FROM AISDraft.ais_message, AISDraft.position_report WHERE AISDraft.ais_message.MMSI=244265000 AND AISDraft.ais_message.Id=AISDraft.position_report.AISMessage_Id;"
        actual = mysqlutils.SQL_runner().run(query)
        self.assertEqual(expected, str(actual))

    # Test for insert_message_batch()
    def test_insert_message_batch_pass(self):
        error = False
        try:
            tmb_dao.tmb_dao().insert_message_batch("sample_input.json")
        except Exception:
            error = True
        self.assertFalse(error)

    # Test successful insert for insert_message_batch()
    def test_insert_message_batch(self):
        tmb_dao.tmb_dao().insert_message_batch("sample_input.json")
        true_output = "[(datetime.datetime(2020, 11, 18, 0, 0), 'Under way using engine')]"
        query = "SELECT AISDraft.ais_message.timestamp, AISDraft.position_report.navigationalstatus FROM AISDraft.ais_message, AISDraft.position_report WHERE AISDraft.ais_message.mmsi=304858000 AND AISDraft.ais_message.id=AISDraft.position_report.aismessage_id;"
        program_output = mysqlutils.SQL_runner().run(query)
        self.assertEqual(true_output, str(program_output))

    # Test for grabbing mySQL datafile location
    def test_request_mysql_files(self):
        output = mysqlutils.SQL_runner().run("SELECT @@GLOBAL.secure_file_priv;")
        output = str(output[0])

        translation_table = dict.fromkeys(map(ord, '(),'), None)
        output = output.translate(translation_table)

        to_remove = "\\"
        pattern = "(?P<char>[" + re.escape(to_remove) + "])(?P=char)+"
        output = re.sub(pattern, r"\1", output)

        print(str(output))
        self.assertEqual(str(output), "\'C:\\Users\\david\\MySQLData\\Uploads\\\'")

if __name__ == '__main__':
    test.main()
