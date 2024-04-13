import json
import psycopg2

from database.config import config


DB_NAME = 'hh'
PARAMS = config()
FILE = '../data/temp/companies.json'


class DBManager:
    '''
    Класс для работы с базой данных
    '''

    def __init__(self, file=FILE, params=PARAMS, db_name=DB_NAME):
        self.__file = file
        self.__params = params
        self.__db_name = db_name

    def __create_database(self) -> None:
        '''
        Создает базу данных
        '''
        conn = psycopg2.connect(dbname='postgres', **self.__params)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(f'DROP DATABASE IF EXISTS {self.__db_name}')
        cur.execute(f'CREATE DATABASE {self.__db_name}')
        conn.commit()
        conn.close()

    def create_tables(self) -> None:
        '''
        Создает таблицы
        '''
        self.__create_database()
        with psycopg2.connect(dbname=self.__db_name, **self.__params) as conn:
            with conn.cursor() as cur:
                cur.execute('''
                    DROP TABLE IF EXISTS company;

                    CREATE TABLE company (
                        company_id INT PRIMARY KEY,
                        company_name VARCHAR(255) NOT NULL,
                        company_link VARCHAR(255)
                    );
                    ''')

                cur.execute('''
                    DROP TABLE IF EXISTS vacancy;

                    CREATE TABLE vacancy (
                        vacancy_id SERIAL PRIMARY KEY,
                        vacancy_title VARCHAR(255) NOT NULL,
                        company_id INT,
                        salary INT,
                        location VARCHAR(50),
                        vacancy_link VARCHAR(255),

                        CONSTRAINT fk_company FOREIGN KEY (company_id) 
                        REFERENCES company(company_id)
                        ON DELETE CASCADE,
                        CONSTRAINT chk_salary CHECK (salary >= 0)
                    );
                    ''')

    def __read_data(self):
        with open(self.__file) as f:
            data = json.load(f)
        return data

    def insert_data(self):
        '''
        Заполняет таблицы
        '''
        data = self.__read_data()
        with psycopg2.connect(dbname=self.__db_name, **self.__params) as conn:
            with conn.cursor() as cur:
                for d in data:
                    company_id = int(d.get('id'))
                    company_name = d.get('name')
                    company_link = d.get('site_url')

                    cur.execute('''
                        INSERT INTO company
                        VALUES (%s, %s, %s)
                        RETURNING company_id
                        ''', (company_id, company_name, company_link))

                    company_id = cur.fetchone()[0]

                    for v in d.get('vacancies'):
                        vacancy_title = v.get('name')
                        salary = v.get('salary').get('from') if v.get('salary') else None
                        location = v.get('area').get('name') if v.get('area') else None
                        vacancy_link = v.get('alternate_url')

                        cur.execute('''
                            INSERT INTO vacancy (vacancy_title, company_id, salary, location, vacancy_link)
                            VALUES (%s, %s, %s, %s, %s)
                            ''', (vacancy_title, company_id, salary, location, vacancy_link))

                conn.commit()

    def get_companies_and_vacancies_count(self) -> list[tuple]:
        '''
        получает список всех компаний и количество вакансий у каждой компании
        '''

        with psycopg2.connect(dbname=self.__db_name, **self.__params) as conn:
            with conn.cursor() as cur:
                cur.execute('''
                SELECT company_name, COUNT(vacancy_id) AS vacancy_count
                FROM company
                JOIN vacancy
                USING (company_id)
                GROUP BY company_name;
                ''')

                data = cur.fetchall()

        return data

    def get_all_vacancies(self) -> list[tuple]:
        '''
        получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию
        '''

        with psycopg2.connect(dbname=self.__db_name, **self.__params) as conn:
            with conn.cursor() as cur:
                cur.execute('''
                SELECT company_name, vacancy_title, salary, vacancy_link
                FROM vacancy
                JOIN company 
                USING (company_id);
                ''')

                data = cur.fetchall()

        return data

    def get_avg_salary(self) -> int:
        '''
        получает среднюю зарплату по вакансиям
        '''
        with psycopg2.connect(dbname=self.__db_name, **self.__params) as conn:
            with conn.cursor() as cur:
                cur.execute('''
                SELECT AVG(salary)::DECIMAL(10, 2)
                FROM vacancy;
                ''')

                data = cur.fetchone()[0]

        return data

    def get_vacancies_with_higher_salary(self) -> list[tuple]:
        '''
        получает список всех вакансий, у которых зарплата выше средней по всем вакансиям
        '''
        with psycopg2.connect(dbname=self.__db_name, **self.__params) as conn:
            with conn.cursor() as cur:
                cur.execute('''
                SELECT * 
                FROM vacancy
                WHERE salary > (
                    SELECT AVG(salary)
                    FROM vacancy
                );
                ''')

                data = cur.fetchall()

        return data

    def get_vacancies_with_keyword(self, keyword: str) -> list[tuple]:
        '''
        получает список всех вакансий, в названии которых содержатся переданные в метод слова
        '''
        with psycopg2.connect(dbname=self.__db_name, **self.__params) as conn:
            with conn.cursor() as cur:
                cur.execute(f'''
                SELECT *
                FROM vacancy
                WHERE LOWER(vacancy_title) LIKE '%{keyword.lower()}%';
                ''')

                data = cur.fetchall()

        return data
