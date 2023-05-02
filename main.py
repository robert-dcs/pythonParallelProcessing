import openpyxl
import psycopg2
import psycopg2.extras
import time
from functools import wraps
from memory_profiler import memory_usage
import multiprocessing as mp
from multiprocessing import cpu_count
import numpy as np
from multiprocessing import Pool

def create_staging_table(cursor) -> None:
    cursor.execute("""
        DROP TABLE IF EXISTS persons;
        CREATE UNLOGGED TABLE persons (
            id SERIAL PRIMARY KEY,
            Name varchar(255) NOT NULL
        );
    """)


def profile(fn):
    @wraps(fn)
    def inner(*args, **kwargs):
        fn_kwargs_str = ', '.join(f'{k}={v}' for k, v in kwargs.items())
        print(f'\n{fn.__name__}({fn_kwargs_str})')

        # Measure time
        t = time.perf_counter()
        retval = fn(*args, **kwargs)
        elapsed = time.perf_counter() - t
        print(f'Time   {elapsed:0.4}')

        # Measure memory
        mem, retval = memory_usage((fn, args, kwargs), retval=True, timeout=200, interval=1e-7)

        print(f'Memory {max(mem) - min(mem)}')
        return retval

    return inner


def convert_list(list):
    return tuple(list)


@profile
def insert_execute_batch(connection, listaDepessoas) -> None:
    with connection.cursor() as cursor:
        create_staging_table(cursor)
        psycopg2.extras.execute_batch(cursor, "INSERT INTO persons (Name) VALUES (%s)", listaDepessoas)

@profile
def insert_person(person, conn=None) -> None:
    sql = ("""insert into persons(name) values (%s);""" % person)

    try:
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print('Eu ruim na insercao', error)
    finally:
        if conn is not None:
            conn.close()

def synchronousProcessing(listOfPeople, conn=None) -> None:
    for person in listOfPeople:
        insert_person(person, conn)

def parallelProcessing(listOfPeople, conn=None):

    df_list = np.array_split(listOfPeople, cpu_count())
    pool = Pool(cpu_count())
    res = pool.map(insert_person(), df_list)
    pool.close()
    pool.join()
    print(res)

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
    insert_execute_batch(connection, convert_list(listOfPeople))
    print("People saved.")

