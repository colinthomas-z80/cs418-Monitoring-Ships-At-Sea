import unittest as test
import create_database
import mysqlutils
import tmb_dao
from mysql.connector import Error


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
    def test_insert_message_pass(self):
        error = False
        ex = '{"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":244265000,"MsgType":"position_report", \
             "Position":{"type":"Point","coordinates":[55.522592,15.068637]},"Status":"Under way using engine","RoT":2.2,"SoG":14.8,"CoG":62,"Heading":61}'
        try:
            tmb_dao.tmb_dao().insert_msg(ex, 0)
        except Error:
            error = True
        self.assertFalse(error)


if __name__ == '__main__':
    test.main()
