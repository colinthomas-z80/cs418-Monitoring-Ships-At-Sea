## AIS Monitoring Ships at Sea

Option A

# Creating the database

Copy the csv data files from canvas into your mysql data directory.
<br><br>
You can see what directory that is in the mysql shell with:

    SELECT @@GLOBAL.secure_file_priv;

Then, run the create_database.py script to create the schema. Enter the insert.sql script into the mysql shell.

