import unittest
from datetime import date
from mysql.connector import connect, Error

from neows import get_nearest_hazardous_asteroid
from neows import get_last_update_date
from neows import make_neows_request
from neows import mqueue, write_data_into_db


class TestNeows(unittest.TestCase):

    def get_value_from_database(self):
        return_val = None
        try:
            db_connection = connect(host="localhost",user="neows_user",passwd="neopass!")
            db_connection.autocommit = True
            db_cursor = db_connection.cursor()
            db_cursor.execute("use neows_db")        
            query = "select miss_distance from neows where neo_reference_id=25039;"
            db_cursor.execute(query)
            for db_val in db_cursor:
                if db_val is not None:
                    return_val = db_val[0]
            return return_val 
        except:
            print("error")
        finally:
            db_cursor.close()
            db_connection.close()

    def delete_value_from_database(self):
        try:
            db_connection = connect(host="localhost",user="neows_user",passwd="neopass!")
            db_connection.autocommit = True
            db_cursor = db_connection.cursor()
            db_cursor.execute("use neows_db")
            query = "delete from neows where neo_reference_id=25039;"
            db_cursor.execute(query)
            return True
        except:
            print("error")

    def test_result_get_nearest_hazardous_asteroid(self):
        expected = ['2021-03-21,1252927.5867523088']
        actual = get_nearest_hazardous_asteroid()
        self.assertEqual(actual, expected)

    def test_noresult_get_nearest_hazardous_asteroid(self):
        expected = [] 
        actual = get_nearest_hazardous_asteroid(0, '2021-05-01', '2021-05-02')
        self.assertEqual(actual, expected)
 
    def test_result_get_last_update_date(self):
        expected = date(year=2021, month=4, day=15)
        actual = get_last_update_date()
        self.assertEqual(actual, expected)
 
    def test_no_result_get_last_update_date(self):
        expected = date(year=2021, month=3, day=14)
        actual = get_last_update_date()
        self.assertNotEqual(actual, expected)

    def test_write_data_into_db(self):
        json_string = "{\"hazardous_asteroid\": 1, \"neo_reference_id\": \"25039\", \"close_approach_date\": \"2022-04-08\", \"miss_distance\": \"25684941.8888328172\"}"
        mqueue.put(json_string)
        mqueue.put("DONE")
        return_val = write_data_into_db('hello')
        actual = self.get_value_from_database()
        expected = 25684941.8888328172
        self.assertEqual(actual, expected)
        self.delete_value_from_database()

    def test_make_neows_request(self):
        start_date = "2020-02-01"
        end_date = "2020-02-01"
        make_neows_request(start_date, end_date)
        return_value = mqueue.get()
        message = "Test value is none."
        print(return_value)
        self.assertIsNotNone(return_value, message)
