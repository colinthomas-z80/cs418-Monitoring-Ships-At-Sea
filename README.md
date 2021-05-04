## AIS Monitoring Ships at Sea

Colin Thomas, David Tao, Iqra Almani

<br>

# Testing the DAO

edit the connection_data.conf file to your local mysql server login. It should look something like this 

    [SQL]

    user=colin
    password=colinspassword
    database=mysql      // this line should match exactly
    host=127.0.0.1


You might need to install the mysql-connector-python package for your python3 installation. You probably already have it though.

Run the unittests.py script to generate the database with the constant data, and test each function of the DAO.

    python3 unittests.py

