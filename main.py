import threading
import openpyxl
import psycopg2
import psycopg2.extras
import time
import concurrent.futures
from psycopg2 import pool
from multiprocessing.dummy import Pool
import os
from queue import Queue

MAX_THREADS = 5

simple_pool = psycopg2.pool.SimpleConnectionPool(
    minconn=1,
    maxconn=20,
    user="postgres",
    password="321",
    host="localhost",
    port="5432",
    database="postgres"
)


def drop_and_create_table():
    connection = simple_pool.getconn()
    connection.autocommit = True
    cursor = connection.cursor()
    cursor.execute("""
            DROP TABLE IF EXISTS person;
            CREATE TABLE person (
                id SERIAL PRIMARY KEY,
                Name varchar(255) NOT NULL
            );
        """)
    simple_pool.putconn(connection)
    print("DB initialized")


def convert_list_to_tuple(list):
    return tuple(list)


def insert_person(person) -> None:
    connection = simple_pool.getconn()
    if connection:
        try:
            cur = connection.cursor()
            cur.execute('INSERT INTO person (name) VALUES (%s)', person)
            connection.commit()
            cur.close()
            simple_pool.putconn(connection)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)


def countRows():
    connection = simple_pool.getconn()
    if connection:
        try:
            cur = connection.cursor()
            cur.execute('SELECT count(*) FROM person')
            result = cur.fetchone()
            print("DB rows: " + str(result[0]))
            cur.close()
            simple_pool.putconn(connection)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)


def getFirstRecord():
    connection = simple_pool.getconn()
    if connection:
        try:
            cur = connection.cursor()
            cur.execute('SELECT name FROM person ORDER BY id ASC')
            result = cur.fetchone()
            print("First record: " + result[0])
            cur.close()
            simple_pool.putconn(connection)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)


def getLastRecord():
    connection = simple_pool.getconn()
    if connection:
        try:
            cur = connection.cursor()
            cur.execute('SELECT name FROM person ORDER BY id DESC')
            result = cur.fetchone()
            print("Last record: " + result[0])
            cur.close()
            simple_pool.putconn(connection)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)


def synchronous_processing(listOfPeople):
    print("\n*--------- Synchronous ----------*")

    drop_and_create_table()
    start = (time.time() * 1000)
    for person in listOfPeople:
        insert_person(person)
    end = (time.time() * 1000)
    elapsed_time = end - start
    print("[Python] Synchronous implementation took " + str(elapsed_time) + " milliseconds.")
    print("[Python] Processed " + str(len(listOfPeople)) + " records.")
    countRows()
    getFirstRecord()
    getLastRecord()


def parallel_processing(listOfPeople):
    print("\n*--------- Parallel 1 ----------*")

    drop_and_create_table()
    start = (time.time() * 1000)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for person in listOfPeople:
            executor.submit(insert_person, [person])
    end = (time.time() * 1000)
    elapsed_time = end - start
    print("[Python] Parallel implementation 1 took " + str(elapsed_time) + " milliseconds.")
    print("[Python] Processed " + str(len(listOfPeople)) + " records.")
    countRows()
    getFirstRecord()
    getLastRecord()


def worker(queue):
    while True:
        person = queue.get()
        if person is None:
            break
        insert_person(person)
        queue.task_done()


def parallel_processing2(listOfPeople):
    print("\n*--------- Parallel 2 ----------*")

    drop_and_create_table()
    MAX_THREADS = os.cpu_count()
    queue = Queue()

    for person in listOfPeople:
        queue.put(person)

    threads = []
    start = (time.time() * 1000)
    for _ in range(MAX_THREADS):
        thread = threading.Thread(target=worker, args=(queue,))
        thread.start()
        threads.append(thread)

    queue.join()

    for _ in range(MAX_THREADS):
        queue.put(None)
    for thread in threads:
        thread.join()

    end = (time.time() * 1000)
    elapsed_time = end - start
    print("[Python] Parallel implementation 2 took " + str(elapsed_time) + " milliseconds.")
    print("[Python] Processed " + str(len(listOfPeople)) + " records.")
    countRows()
    getFirstRecord()
    getLastRecord()


def parallel_processing3(listOfPeople):
    print("\n*--------- Parallel 3 ----------*")

    drop_and_create_table()
    start = (time.time() * 1000)
    with Pool() as pool:
        pool.map(insert_person, listOfPeople)

    end = (time.time() * 1000)
    elapsed_time = end - start
    print("[Python] Parallel implementation 3 took " + str(elapsed_time) + " milliseconds.")
    print("[Python] Processed " + str(len(listOfPeople)) + " records.")
    countRows()
    getFirstRecord()
    getLastRecord()


if __name__ == '__main__':
    sampleSize = 1000
    while (sampleSize <=
           1000000):
        print("\n*--------- New execution: sample size " + str(sampleSize) + " ----------*")
        listOfPeople = []
        wb = openpyxl.load_workbook(filename="sample/data" + str(sampleSize) + ".xlsx")
        for person in wb['Sheet1'].iter_rows(values_only=True):
            listOfPeople.append(person)

        print("First record from sample: " + str(listOfPeople[0]))
        print("Last record from sample: " + str(listOfPeople[len(listOfPeople) - 1]))
        for x in range(4):
            print("*--------- " + str(x) + " execution ---------*")
            synchronous_processing(convert_list_to_tuple(listOfPeople))
            parallel_processing(convert_list_to_tuple(listOfPeople))
            parallel_processing2(convert_list_to_tuple(listOfPeople))
            parallel_processing3(convert_list_to_tuple(listOfPeople))


        sampleSize *= 10
