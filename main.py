import openpyxl
import psycopg2
import psycopg2.extras
import time
from multiprocessing import cpu_count
from multiprocessing import Pool

def create_staging_table() -> None:
    connection = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password=321,
    )
    connection.autocommit = True
    cursor = connection.cursor()
    cursor.execute("""
        DROP TABLE IF EXISTS persons;
        CREATE UNLOGGED TABLE persons (
            id SERIAL PRIMARY KEY,
            Name varchar(255) NOT NULL
        );
    """)
    print("Table dropped and created")

def convert_list(list):
    return tuple(list)

def convert_string(string):
    return tuple(string)

def insert_execute_batch(connection, listaDepessoas) -> None:
    with connection.cursor() as cursor:
        create_staging_table(cursor)
        psycopg2.extras.execute_batch(cursor, "INSERT INTO persons (Name) VALUES (%s)", listaDepessoas)

def insert_person(person, conn=None) -> None:

    sql = "INSERT INTO persons(Name) VALUES (%s)"

    connection = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password=321,
    )

    connection.autocommit = True
    name = person[0]
    try:
        cur = connection.cursor()
        # execute the INSERT statement
        cur.execute('INSERT INTO persons (name) VALUES (%s)', name)
        print(person + "saved")
        connection.commit()
        connection.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(type(name))
        print(name)
        print('Eu ruim na insercao', error)
    finally:
        print('Connection closed')
        if connection is not None:
            connection.close()

def synchronous_processing(listOfPeople) -> None:
    create_staging_table()
    start = time.time()
    for person in listOfPeople:
        insert_person(person)
    end = time.time()
    elapsedTime = end - start
    print("Processed " + len(listOfPeople) + " rows synchronous and took " + elapsedTime + " seconds")

def parallel_processing(listOfPeople):
    start = time.time()
    for person in listOfPeople:
        pool = Pool(cpu_count())
        pool.map(insert_person, person)
        pool.close()
        pool.join()

    end = time.time()
    elapsedTime = end - start
    print("Processed " + len(listOfPeople) + " rows parallel and took " + elapsedTime + " seconds")

def format_person(person):
    return str(person).replace("'", '"')

if __name__ == '__main__':
    listOfPeople = []
    wb = openpyxl.load_workbook(filename="data.xlsx")
    for person in wb['Sheet1'].iter_rows(values_only=True):
        listOfPeople.append(person)

    connection = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password=321,
    )
    connection.autocommit = True
    # insert_execute_batch(connection, convert_list(listOfPeople))
    synchronous_processing(listOfPeople)
    # parallel_processing(convert_list(listOfPeople))
    print("People saved.")


