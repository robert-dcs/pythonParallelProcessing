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

def create_staging_table():
    connection = simple_pool.getconn()
    connection.autocommit = True
    cursor = connection.cursor()
    cursor.execute("""
            DROP TABLE IF EXISTS persons;
            CREATE UNLOGGED TABLE persons (
                id SERIAL PRIMARY KEY,
                Name varchar(255) NOT NULL
            );
        """)
    simple_pool.putconn(connection)
    print("Table dropped and created")

def convert_list(list):
    return tuple(list)

def insert_person(person) -> None:
    connection = simple_pool.getconn()
    if connection:
        sql = "INSERT INTO persons(Name) VALUES (%s)"

        connection.autocommit = True
        try:
            cur = connection.cursor()
            # execute the INSERT statement
            cur.execute('INSERT INTO persons (name) VALUES (%s)', person)
            connection.commit()
            simple_pool.putconn(connection)
        except (Exception, psycopg2.DatabaseError) as error:
            print('Deu ruim na insercao', error)

def synchronous_processing(listOfPeople) -> None:
    create_staging_table()
    start = time.time()
    for person in listOfPeople:
        insert_person(person)
    end = time.time()
    elapsedTime = end - start
    print("Processed " + str(len(listOfPeople)) + " rows synchronous and took " + str(elapsedTime)  + " seconds")

def parallel_processing(listOfPeople):
    create_staging_table()
    start = time.time()
    print("Running threaded:")
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for person in listOfPeople:
            futures.append(executor.submit(insert_person, [person]))
        for future in concurrent.futures.as_completed(futures):
            future.result()
        end = time.time()
        elapsedTime = end - start
        print("Processed " + str(len(listOfPeople)) + " rows parallel and took " + str(elapsedTime) + " seconds")
def format_person(person):
    return str(person).replace("'", '"')

if __name__ == '__main__':
    listOfPeople = []
    wb = openpyxl.load_workbook(filename="data1000000.xlsx")
    for person in wb['Sheet1'].iter_rows(values_only=True):
        listOfPeople.append(person)

        # connection = psycopg2.connect(
        #     host="localhost",
        #     database="postgres",
        #     user="postgres",
        #     password=321,
        # )
     # connection.autocommit = True
     # insert_execute_batch(connection, convert_list(listOfPeople))
    synchronous_processing(convert_list(listOfPeople))
    parallel_processing(convert_list(listOfPeople))
    print("People saved.")


