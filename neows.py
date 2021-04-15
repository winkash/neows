#!/usr/bin/python3
import sys
import json
import requests
import datetime
from datetime import timedelta
from multiprocessing import Process, Queue
from mysql.connector import connect, Error
import time
from concurrent.futures import ThreadPoolExecutor

mqueue = Queue()

def get_nearest_hazardous_asteroid(limit = 1, start_date = None, end_date = None):
    return_result = []
    try:
        db_connection = connect(host="localhost",user="neows_user",passwd="neopass!")
        db_connection.autocommit = True
        db_cursor = db_connection.cursor()
        db_cursor.execute("use neows_db")
        query = "select close_approach_date, miss_distance from neows where is_potentially_hazardous_asteroid = 1"
        if start_date is not None:
            query += " and close_approach_date > '" + start_date + "'"
        if end_date is not None:
            query += " and close_approach_date < '" + end_date + "'"
        query += " order by miss_distance limit 0, " + str(limit)
        print(query)
        db_cursor.execute(query);
        for db in db_cursor:
            date_obj = datetime.datetime.combine(db[0],datetime.datetime.min.time())
            date_str = date_obj.strftime('%Y-%m-%d')
            return_result.append(date_str+","+str(db[1]))
        return return_result
    finally:
        db_cursor.close()
        db_connection.close()

def get_last_update_date():
    return_date = None
    try:
        db_connection = connect(host="localhost",user="neows_user",passwd="neopass!")
        db_connection.autocommit = True
        db_cursor = db_connection.cursor()
        db_cursor.execute("use neows_db")
        query = "select close_approach_date from neows order by close_approach_date desc limit 1;"
        db_cursor.execute(query);
        #if db_cursor.rowcount == 0:
            #return_date_obj = datetime.datetime.combine(datetime.date.today(), datetime.datetime.min.time())
            #return return_date_obj.strftime('%Y-%m-%d')
            #return None
        for db in db_cursor:
            if db is not None:
                return_date = db
        if return_date is None:
            return None
        return return_date[0]
    finally:
        db_cursor.close()
        db_connection.close()
        

def write_data_into_db(messages):
     try:
         db_connection = connect(host="localhost",user="neows_user",passwd="neopass!")
         db_connection.autocommit = True
         db_cursor = db_connection.cursor()
         db_cursor.execute("use neows_db")
         query = 'INSERT INTO neows(neo_reference_id, is_potentially_hazardous_asteroid, miss_distance, close_approach_date) values (%s, %s, %s, %s)'
         row_data = []
         counter = 0
         while True:
            if not mqueue.empty():
                token = mqueue.get(timeout=60)
                if token == "DONE":
                    print("committing values before done")
                    db_cursor.executemany(query, row_data)
                    return True
                json_data = json.loads(token)
                #print("1")
                tple_data = (json_data["neo_reference_id"], json_data["hazardous_asteroid"],json_data["miss_distance"],json_data["close_approach_date"])
                #print("2")
                row_data.append(tple_data) 
                #print("3")
                counter += 1
                #print("4")
                #print(tple_data)
                #db_cursor.execute(query,tple_data)
                if len(row_data) > 50:
                    print("committing values")
                    db_cursor.executemany(query, row_data)
                    row_data = []
                #print(5)
                #print("counter in write data is {} ".format(counter))
     except mysql.connector.ProgrammingError as err:
         print(err.errno)
         print(err.msg)
     except TypeError as e:
         print(e)
     finally:
         db_cursor.close()
         db_connection.close()
                

def make_neows_request(startdate, enddate):
    try:
        startdate_obj = datetime.datetime.strptime(startdate, '%Y-%m-%d')
        enddate_obj = datetime.datetime.strptime(enddate, '%Y-%m-%d')
        delta = enddate_obj - startdate_obj
        delta = delta.days
        neows_url = 'https://api.nasa.gov/neo/rest/v1/feed?start_date={start_date}&end_date={end_date}&detailed=true&api_key=Vz9ZTFaUcBlMKxf9AjOHTBAO4MKw7h7dyv0XMQKp'.format(start_date=startdate,end_date=enddate)
        print(neows_url)
        response = requests.get(neows_url, timeout=20)
        json_data = response.json() if response and response.status_code == 200 else None
        if json_data is not None:
            currentdate = startdate
            currentdate_obj = startdate_obj
            counter = 0
            for i in range(delta+1):
                print("current date - {0}".format(currentdate))
                near_earth_objects = json_data["near_earth_objects"]
                objects_len = len(near_earth_objects[currentdate])
                for i in range(objects_len):
                    data = {}
                    data['hazardous_asteroid'] = near_earth_objects[currentdate][i]["is_potentially_hazardous_asteroid"]
                    #data['hazardous_asteroid'] = near_earth_objects[currentdate][i]["is_potentially_hazardous_asteroid"]
                    data['neo_reference_id'] = near_earth_objects[currentdate][i]["neo_reference_id"]
                    data['close_approach_date'] = near_earth_objects[currentdate][i]["close_approach_data"][0]["close_approach_date"]
                    data['miss_distance'] = near_earth_objects[currentdate][i]["close_approach_data"][0]["miss_distance"]["miles"]
                    send_data = json.dumps(data)
                    mqueue.put(send_data)
                    counter += 1
                currentdate_obj = currentdate_obj+timedelta(days=1)
                currentdate = currentdate_obj.strftime('%Y-%m-%d')
        print(counter)
    except requests.exceptions.HTTPError as errh:
        print(errh)
    except requests.exceptions.ConnectionError as errc:
        print(errc)
    except requests.exceptions.Timeout as errt:
        print(errt)
    except requests.exceptions.RequestException as err:
        print(err)

def validate_input_date(input_date_obj):
    todaydate_obj = datetime.datetime.combine(datetime.date.today(), datetime.datetime.min.time())
    if input_date_obj > todaydate_obj:
        return False
    return True

if __name__ == '__main__':
    user_input = input("Do you want to check nearest hazardous asteroid(Y/N): ")
    if user_input == "Y":
        result = get_nearest_hazardous_asteroid()
        print(result)
        sys.exit(1)
    last_update_date_obj = get_last_update_date()
    if last_update_date_obj is not None:
        startdate_obj = datetime.datetime.combine(last_update_date_obj,datetime.datetime.min.time())
        startdate_obj = startdate_obj+timedelta(days=1)
        startdate = last_update_date_obj.strftime('%Y-%m-%d')
        print(startdate)
        #enddate_obj = datetime.datetime.combine(datetime.date.today(), datetime.datetime.min.time())
        #enddate = enddate_obj.strftime('%Y-%m-%d')
        #print(enddate)
    else:
        startdate = input("Backfill Date(YYYY-MM-DD): ")
        try:
            startdate_obj = datetime.datetime.strptime(startdate, '%Y-%m-%d')
            if not validate_input_date(startdate_obj):
                raise ValueError('Wrong date format')
        except ValueError:
            try:
                startdate = input("Enter a valid date before today(YYYY-MM-DD): ")
                startdate_obj = datetime.datetime.strptime(startdate, '%Y-%m-%d')
                if not validate_input_date(startdate_obj):
                    raise ValueError('Wrong date format')
            except ValueError:
                sys.exit("Invalid date provided in 2 attempts")
        #startdate_obj = datetime.datetime.strptime(startdate, '%Y-%m-%d')
        
    print(startdate_obj)
    enddate_obj = datetime.datetime.combine(datetime.date.today(), datetime.datetime.min.time())
    enddate = enddate_obj.strftime('%Y-%m-%d')
    print(enddate)
    pqueue = Queue()
    #startdate = input("Start Date: ")
    #enddate = input("End Date: ")
    #startdate_obj = datetime.datetime.strptime(startdate, '%Y-%m-%d')
    #enddate_obj = datetime.datetime.strptime(enddate, '%Y-%m-%d')
    delta = enddate_obj - startdate_obj
    delta = delta.days
    print(delta)

    temp_date_obj = startdate_obj+timedelta(days=7)
    while temp_date_obj < enddate_obj:
        data = {}
        data['start_date'] = startdate_obj.strftime('%Y-%m-%d')
        data['end_date'] = temp_date_obj.strftime('%Y-%m-%d')
        pqueue.put(json.dumps(data))
        startdate_obj = temp_date_obj+timedelta(days=1)
        temp_date_obj = temp_date_obj+timedelta(days=8)
    data = {}
    data['start_date'] = startdate_obj.strftime('%Y-%m-%d')
    data['end_date'] = enddate_obj.strftime('%Y-%m-%d')
    pqueue.put(json.dumps(data))
    pqueue.put("DONE")
    consumer_task = False
    pool = ThreadPoolExecutor(1)
    #(db_connection, db_cursor) = make_sql_connection()
    future = pool.submit(write_data_into_db, ("hello"))
    producer_complete = False
    while True:
        msg = pqueue.get()
        if (msg == 'DONE'):
            producer_complete = True
            break
        json_str = json.loads(msg)
        make_neows_request(json_str['start_date'], json_str['end_date'])
    while not mqueue.empty():
         time.sleep(20)
    if producer_complete:
        mqueue.put('DONE')

#if future.done():
#   print("complete")
#write_data_into_db("hello")
#db_cursor.close()
#db_connection.close()
