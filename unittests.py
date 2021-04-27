import unittest as test
import create_database
import mysql.connector

class ais_unit_tests(test.TestCase):

    # Tests unit tester
    def test_if_true(self):
        self.assertTrue(True, "Pass")
        
    def test_if_false(self):
        self.assertFalse(False, "Pass")
        
    # Unit Tests for create_database.py
    def test_create_database(self):
        create_database
        error = False
        client = mysql.connector.connect(host = 'localhost', user = 'iqrasuhail', passwd = 'marviabroforlife')
        cursor = client.cursor()
        try:
            cursor.execute("USE AISDraft")
        except:
            error = True
        self.assertFalse(error)
    

if __name__ == '__main__':
    test.main()
