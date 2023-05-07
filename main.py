import openpyxl
import psycopg2
import psycopg2.extras
import time
import concurrent.futures
from psycopg2 import pool

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
            DROP TABLE IF EXISTS persons;
            CREATE TABLE persons (
                id SERIAL PRIMARY KEY,
                Name varchar(255) NOT NULL
            );
        """)
    simple_pool.putconn(connection)
    print("Table dropped and created")

def convert_list_to_tuple(list):
    return tuple(list)

def insert_person(person) -> None:
    connection = simple_pool.getconn()
    if connection:
        try:
            cur = connection.cursor()
            cur.execute('INSERT INTO persons (name) VALUES (%s)', person)
            connection.commit()
            simple_pool.putconn(connection)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

def synchronous_processing(listOfPeople) -> None:
    drop_and_create_table()
    start = (time.time() * 1000)
    for person in listOfPeople:
        insert_person(person)
    end = (time.time() * 1000)
    elapsed_time = end - start
    print("Synchronous implementation took " + elapsed_time + " milliseconds and processed " + len(listOfPeople) + " records.\n")

def parallel_processing(listOfPeople):
    drop_and_create_table()
    start = (time.time() * 1000)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for person in listOfPeople:
            futures.append(executor.submit(insert_person, [person]))
        for future in concurrent.futures.as_completed(futures):
            future.result()
        end = (time.time() * 1000)
        elapsed_time = end - start
        print("Synchronous implementation took " + elapsed_time + " milliseconds and processed " + len(listOfPeople) + " records.\n")

if __name__ == '__main__':
    listOfPeople = []
    wb = openpyxl.load_workbook(filename="data1000000.xlsx")
    for person in wb['Sheet1'].iter_rows(values_only=True):
        listOfPeople.append(person)

    synchronous_processing(convert_list_to_tuple(listOfPeople))
    parallel_processing(convert_list_to_tuple(listOfPeople))
    print("People saved.")


