Near Earth Object

NeoWs is a Near Earth Object Web Service provided by NASA JPL Asteroid team. This project attempts to capture all Asteroids approaching earth and identifies any hazardous asteroid and distance from the earth. 

Application ETL when invoked

* Queries the API for new data, based on the last record of capture. If there are no records in the database, backfill from a user provided date.
* Captures relevant information about potentially hazardous NEO's, including the close approach date and miss distance and writes to db

Software requirements

* Python 3.7
* MySql 8.0
* Unix/Mac

Mysql DB creation

As a root user please execute the following statement to setup application user_id/password

* CREATE USER 'neows_user'@'%' IDENTIFIED BY 'neopass!';
* GRANT ALL PRIVILEGES ON neows_db.* TO 'neows_user'@'%' WITH GRANT OPTION;

* source scripts/neows.sql

Executing ETL

* python3.7 neows.py

Executing ETL testcases

* python3.7 -m unittest neows_test.py

