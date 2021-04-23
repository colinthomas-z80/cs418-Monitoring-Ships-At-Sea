#!/usr/bin/python3

# CS418, Spring 2021
# Nicolas Renet

# Exercises in SQL (covering Week #8)


import unittest
import mysql.connector
from mysql.connector import errorcode

import sys, re
from mysqlutils import SQL_runner


# Q1.  Find the client code of those clients who own 2 vessels or more
# (Hint: query the VESSEL table, discarding those records with a NULL owner code, and using the GROUP BY and HAVING clauses.)
QUERY1="""
SELECT 'NOT IMPLEMENTED'
"""

# Q2.  For those clients who own 2 vessels or more, retrieve the Client Name, and the IMO and Name of vessels with tonnage > 3,000 tonnes, sorted by client name and IMO.
# (Hint: use the previous answer to write a subquery, whose result can be used in a IN clause; cross the CLIENT and VESSEL table.)
QUERY2="""
SELECT 'NOT IMPLEMENTED'
"""

# Q3.  Count the total number of vessels with tonnage > 3,000 tonnes, but only for those  clients who own two vessels or more
# (Hint: 
#    1. Reuse your answer to Q1, in order to find how many vessels are owned by each client
#    2. Use this result (either through a FROM + subquery clause, or with an intermediate table) to query the VESSEL table again, but this time 
#       keeping only the ships whose tonnage exceeds 3000 tons and whose owners are in the first list
# )
QUERY3="""
SELECT 'NOT IMPLEMENTED'
"""

# Q4. Find the number of vessels that are _not_ involved in a voyage
# Hint: 1. Cross the the VESSEL and VOYAGE tables to find those vessels that _are_ involved in a voyage
#       2. Filter those IMOs that not appear in the first list (through NOT IN + subquery, for instance)
QUERY4="""
SELECT 'NOT IMPLEMENTED'
"""



############################### DO NOT MODIFY AFTER THIS LINE ####################


class SQL_UnitTest( unittest.TestCase ):

    def test_query_01_size( self ):

        rs = SQL_runner().run( QUERY1)
        self.assertEqual( len(rs), 12) 

    def test_query_01_content( self ):

        rs = SQL_runner().run( QUERY1)
        tuples = self.table_to_rows("""
                        +-------+
                        | Code  |
                        +-------+
                        |  3537 |
                        |  5657 |
                        |  5958 |
                        |  7247 |
                        |  8486 |
                        |  9633 |
                        | 10212 |
                        | 15885 |
                        | 16399 |
                        | 16693 |
                        | 17462 |
                        | 18500 |
                        +-------+""")

        self.assertEqual( rs, tuples)


    def test_query_02_size( self ):

        rs = SQL_runner().run( QUERY2)
        self.assertEqual( len(rs), 23) 

    def test_query_02_content( self ):

        rs = SQL_runner().run( QUERY2)
        tuples = self.table_to_rows("""
            +-----------------------------------------+---------+-----------------+
            | Name                                    | IMO     | Name            |
            +-----------------------------------------+---------+-----------------+
            | Allseas Marine Contractors              | 9491238 | Dayang Century  |
            | Allseas Marine Contractors              | 9589085 | Seas 1          |
            | Beech Shipping                          | 9173305 | Pacific Cape    |
            | Beech Shipping                          | 9681376 | Stanford Eagle  |
            | China Shipping Container Lines          | 9262144 | Xin Su Zhou     |
            | China Shipping Container Lines          | 9310056 | Xin Nan Sha     |
            | China Shipping Container Lines          | 9334935 | Xin Ya Zhou     |
            | Er Schiffahrt                           | 9231248 | Msc India       |
            | Er Schiffahrt                           | 9337274 | Mozart          |
            | Er Schiffahrt                           | 9519078 | Star Marianne   |
            | Mizuho Sangyo                           | 9605009 | Cape Sunrise    |
            | Mizuho Sangyo                           | 9650779 | Frontier Youth  |
            | Nyk Line                                | 9206372 | Onga            |
            | Nyk Line                                | 9416989 | Nyk Romulus     |
            | Nyk Line                                | 9675585 | Morning Cherry  |
            | Nyk LNG Shipmanagement                  | 9085637 | Doha            |
            | Nyk LNG Shipmanagement                  | 9085649 | Al Zubarah      |
            | Ocean Grow International Shipmanagement | 8504662 | Leader          |
            | Ocean Grow International Shipmanagement | 9364904 | Chang Hong      |
            | Shoei Kisen                             | 9446570 | Berge Ishizuchi |
            | Shoei Kisen                             | 9477919 | Morning Camilla |
            | Vosco                                   | 9192026 | Stc Athena      |
            | Vosco                                   | 9375642 | Lan Ha          |
            +-----------------------------------------+---------+-----------------+""")

        self.assertEqual( rs, tuples)


    def test_query_03_type( self ):

        rs = SQL_runner().run( QUERY3)
        self.assertTrue( self.check_for_int( rs ))

    def test_query_03_content( self ):

        rs = SQL_runner().run( QUERY3)
        self.assertEqual( rs, [(23,)])
                
    def test_query_04_type( self ):

        rs = SQL_runner().run( QUERY4)
        self.assertTrue( self.check_for_int( rs ))

    def test_query_04_content( self ):

        rs = SQL_runner().run( QUERY4)
        self.assertEqual( rs, [(499,)])


    def table_to_rows(self, table_string):
        
        rows = []
        first = True
        digit_pattern = re.compile(r'\d+')

        def convert( x ):
            x = x.strip()
            if digit_pattern.match( x ):
                return int(x)
            return x

        for row in table_string.split('\n')[4:-1]:
            tpl = tuple( [ convert(r) for r in row.split('|')[1:-1] ])
            rows.append( tpl )
        return rows

    def check_for_int(self, rs ):
        return len(rs)==1 and len(rs[0])==1 and type(rs[0][0]) is int 


if __name__ == '__main__':
    unittest.main()


