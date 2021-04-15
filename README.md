Near Earth Object

NeoWs is a Near Earth Object Web Service provided by NASA JPL Asteroid team. This project attempts to capture all Asteroids approaching earth and identifies any hazardous asteroid and distance from the earth. 

Application ETL when invoked

* Query the API for new data, based on the last record of capture. If there are no records in the database, backfill from a user provided date.
* Capture relevant information about potentially hazardous NEO's, including the close approach date and miss distance.

Software requirements

* Python 3.7
* MySql 8.0
* Unix/Mac

Mysql DB creation

* source scripts/neows.sql

Executing ETL

* python3.7 neows.py

Executing ETL testcases

* python3.7 -m unittest neows_test.py

