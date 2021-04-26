## AIS Monitoring Ships at Sea

Option A

# Creating the script file

Replace non-escaped quotes in the VESSELS.CSV file with 

    sed s/\'/''/g new.sql > new2.sql

Insert parentheses and commas for proper sql INSERT format with

    awk '{print "("$0"),"}' PORT.csv > new.sql

Replace delimiting semicolons in PORT.CSV with

    sed 's/;/,/g' PORT.csv > new.csv

Replace \n escapes with NULL with

    sed 's/\\N/NULL/g' Vessel_Data.mysql > new.mysql