import os
import random
import string
import time
from sys import argv

import mysql.connector as mysql
from dotenv import load_dotenv

load_dotenv()

OBJECT_COUNT = 100000
ALPHABET = {letter: 0 for letter in string.ascii_lowercase}
LETTERS = list(ALPHABET.keys())
MIN_NUM = 1
MAX_NUM = 10


class DataBaseCRUD:
    """Класс для работы с БД."""

    def __init__(self) -> None:
        self.host = os.getenv('HOST_DB')
        self.user = os.getenv('USER_DB')
        self.password = os.getenv('PASSWORD_DB')
        self.connection = self.make_connection_with_db()
        self.cursor = self.make_cursor()

    def make_connection(self):
        """Подключение к серверу."""
        try:
            connection = mysql.connect(
                host=os.getenv('HOST_DB'),
                user=os.getenv('USER_DB'),
                password=os.getenv('PASSWORD_DB')
                )
            return connection
        except mysql.Error as error:
            print(f'При подключении к MySQL возникла ошибка: {error}...')

    def make_connection_with_db(self):
        """Создание подключения к БД."""
        try:
            connection = mysql.connect(host=os.getenv('HOST_DB'),
                                       user=os.getenv('USER_DB'),
                                       password=os.getenv('PASSWORD_DB'),
                                       database=os.getenv('DATABASE'))
        except mysql.Error as error:
            if error.errno == 1049:
                connection = self.make_connection()
                cursor = connection.cursor()
                stmt = '''CREATE DATABASE IF NOT EXISTS employees;'''
                cursor.execute(stmt)
                connection.close()
                connection = mysql.connect(
                    host=os.getenv('HOST_DB'),
                    user=os.getenv('USER_DB'),
                    password=os.getenv('PASSWORD_DB'),
                    database=os.getenv('DATABASE'))

        return connection

    def make_cursor(self):
        """Создание курсора."""
        return self.connection.cursor()

    def create_db(self, cursor):
        """Создание БД employees."""
        stmt = '''CREATE DATABASE IF NOT EXISTS employees;'''
        cursor.execute(stmt)

    def create_table_gender(self):
        """Создание таблицы gender."""
        stmt_gender = '''
        CREATE TABLE IF NOT EXISTS gender
        (
            gender_id INTEGER PRIMARY KEY AUTO_INCREMENT,
            type CHAR(255) NOT NULL
        );
        '''
        self.cursor.execute(stmt_gender)

    def create_table_person(self):
        """Создание таблицы person."""
        stmt_person = '''
        CREATE TABLE IF NOT EXISTS person
        (
            person_id INTEGER PRIMARY KEY AUTO_INCREMENT,
            full_name CHAR(255) NOT NULL,
            birthday DATE NOT NULL,
            gender_id INTEGER,
            FOREIGN KEY (gender_id) REFERENCES gender(gender_id)
        );
         '''
        self.cursor.execute(stmt_person)

    def insert_data_to_gender(self):
        """Вставка данных в таблицу gender."""
        stmt = '''INSERT INTO gender (type) VALUES(%s)'''
        data = (
            ('male', ),
            ('female', ),
        )
        self.cursor.executemany(stmt, data)
        self.connection.commit()

    def insert_data_to_person(self, employee: 'Employee'):
        """Вставка данных в таблицу person."""
        data = employee.get_data()
        stmt = '''
        INSERT INTO person
        (full_name, birthday, gender_id)
        VALUES(%s, %s, %s);
        '''
        self.cursor.execute(stmt, data)
        self.connection.commit()

    def get_unique_rows(self):
        """Получение уникальных записей."""
        query = '''
        SET sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));'''
        self.cursor.execute(query)

        query = '''
        SELECT full_name, birthday, g.type,
                TIMESTAMPDIFF(year, birthday, now()) as age
        FROM person as p
        LEFT JOIN gender as g ON p.gender_id = g.gender_id
        GROUP BY p.full_name, p.birthday
        ORDER BY p.full_name;
        '''
        self.cursor.execute(query)

        results = self.cursor.fetchall()
        with open('report_unique.txt', 'w', encoding='utf-8') as f:
            for result in results:
                row = (f'{result[0]} {result[1]} {result[2]} {result[3]}')
                f.writelines(row + '\n')

    def get_filtered_rows(self):
        """Получения отфильтрованных записей."""
        query = '''
        SELECT full_name, birthday, g.type,
                TIMESTAMPDIFF(year, birthday, now()) as age
        FROM person as p
        LEFT JOIN  gender as g ON p.gender_id = g.gender_id
        WHERE full_name LIKE 'F%' AND g.type = 'male';
        '''
        self.cursor.execute(query)
        before_time = time.time()
        results = self.cursor.fetchall()

        with open('report_first_letter_f.txt', 'w', encoding='utf-8') as f:
            for result in results:
                row = (f'{result[0]} {result[1]} {result[2]} {result[3]}')
                f.writelines(row + '\n')

            f.writelines('Время выполнения запроса: '
                         f'{round(time.time() - before_time, 3)}с.')

    def close_connection(self):
        """Закрытие соединения с БД."""
        if self.connection:
            self.connection.close()
            self.connection = None


class Employee:
    """Класс работника."""

    def __init__(
            self, full_name, date_of_birthday, gender
            ):
        self.__full_name = full_name
        self.__date_of_birthday = date_of_birthday
        self.__gender = '1'
        if gender.lower() == 'female':
            self.__gender = '2'

    def get_data(self):
        return (
            self.__full_name,
            self.__date_of_birthday,
            self.__gender
        )


def make_random_letters_string():
    """Возвращает строку из случайных букв."""
    return ''.join(random.sample(LETTERS, random.randint(MIN_NUM, MAX_NUM)))


def create_employee():
    """Создание экземпляра класса Работник."""
    min_value = min(ALPHABET.values())
    for employee_num in range(OBJECT_COUNT):
        first_letter_for_last_name = random.choice(LETTERS)
        while ALPHABET[first_letter_for_last_name] != min_value:
            first_letter_for_last_name = random.choice(LETTERS)
        ALPHABET[first_letter_for_last_name] += 1
        min_value = min(ALPHABET.values())
        last_name = (
            first_letter_for_last_name
            + make_random_letters_string()
            ).capitalize()
        first_name = make_random_letters_string().capitalize()
        second_name = make_random_letters_string().capitalize()
        full_name = last_name + ' ' + first_name + ' ' + second_name
        gender = 'male'
        date_of_birthday = f'{random.randint(1990, 2000)}-01-01'

        if employee_num % 2 != 0:
            gender = 'female'

        yield Employee(
            full_name=full_name,
            gender=gender,
            date_of_birthday=date_of_birthday
            )


def main():
    database = DataBaseCRUD()
    if len(argv) > 1:
        if argv[1] == '1':
            database.create_table_gender()
            database.insert_data_to_gender()
            database.create_table_person()

        elif argv[1] == '2':
            employee = Employee(full_name=argv[2],
                                date_of_birthday=argv[3],
                                gender=argv[4])
            database.insert_data_to_person(employee=employee)
        elif argv[1] == '3':
            database.get_unique_rows()
        elif argv[1] == '4':
            employees = create_employee()
            for _ in range(OBJECT_COUNT):
                database.insert_data_to_person(next(employees))
        elif argv[1] == '5':
            database.get_filtered_rows()

    database.close_connection()


if __name__ == '__main__':
    main()
