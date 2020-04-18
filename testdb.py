import time
import pymysql
import cryptography
import psycopg2
import pymssql

test = int(input("Выберите базу данных для тестирования (1 - MySQL, 2 - PostgreSQL, 3 - MSSQL): "))

if (test not in [1, 2, 3]):
    while (test not in [1, 2, 3]):
        test = int(input("Выберите базу данных для тестирования (1 - MySQL, 2 - PostgreSQL, 3 - MSSQL): "))

N = int(input("Введите количество тестов: "))

if (N <= 0):
    while (N <= 0):
        N = int(input("Введите количество тестов: "))

exit = True
connection = None

try:
    if (test == 1):
        print("Тестирование MySQL")
        connection = pymysql.connect(host='127.0.0.1', user='root', password='1234', db='testdb', charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

    if (test == 2):
        print("Тестирование PostgreSQL")
        connection = psycopg2.connect(dbname='testdb', user='postgres', password='1234', host='127.0.0.1')

    if (test == 3):
        print("Тестирование MSSQL")
        connection = pymssql.connect('.', 'root', '1234', 'testdb')

    n = 0
    time1 = 0
    time2 = 0
    time3 = 0
    time4 = 0
    time5 = 0
    time6 = 0
    time7 = 0

    while (n < N):
        start_time = time.time()
        clock1 = 0
        clock2 = 0
        clock3 = 0
        clock4 = 0
        clock5 = 0

        with connection.cursor() as cursor:
            f = None
            if (test == 1):
                f = open('datam1.txt')

            if (test == 2):
                f = open('datap1.txt')

            if (test == 3):
                f = open('datams1.txt')

            i = 1
            for sql in f:
                cursor.execute(sql)

                if (i == 4):
                    clock1 = time.time()

                if (i == 26):
                    clock2 = time.time()

                if (i == 46):
                    clock3 = time.time()

                if (i == 51):
                    clock4 = time.time()

                if (i == 71):
                    clock5 = time.time()

                connection.commit()
                i = i + 1

        end_time = time.time()
        time1 = time1 + (clock1 - start_time)
        time2 = time2 + (clock2 - clock1)
        time3 = time3 + (clock3 - clock2)
        time4 = time4 + (clock4 - clock3)
        time5 = time5 + (clock5 - clock4)
        time6 = time6 + (end_time - clock5)
        time7 = time7 + (end_time - start_time)
        cursor.close()
        f.close()
        n = n + 1

    print("Время выполнения  запросов create:  ", time1 / N)
    print("Время выполнения  запросов insert:  ", time2 / N)
    print("Время выполнения  запросов select1:  ", time3 / N)
    print("Время выполнения  запросов update:  ", time4 / N)
    print("Время выполнения  запросов select2:  ", time5 / N)
    print("Время выполнения  запросов drop:  ", time6 / N)
    print("Общее время в секундах:  ", time7 / N)

except pymysql.err.OperationalError:
    print("Ошибка соединения с базой данных!")
    exit = False

except psycopg2.OperationalError:
    print("Ошибка соединения с базой данных!")
    exit = False

except pymssql.OperationalError:
    print("Ошибка соединения с базой данных!")
    exit = False

except FileNotFoundError:
    print("Файл не найден!")

finally:
    if (exit == True):
        connection.close()
        print("Соединение закрыто")
