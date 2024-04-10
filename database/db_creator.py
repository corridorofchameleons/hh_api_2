import json
import psycopg2

from database.db_config import config

DB_NAME = 'hh'
params = config()
FILE = '../data/temp/companies.json'


class DBCreator:
    '''
    Создает БД, таблицы, заполняет их
    '''
    @staticmethod
    def __read_data():
        with open(FILE) as f:
            data = json.load(f)
        return data

    @staticmethod
    def __create_database() -> None:
        '''
        Создает базу данных
        '''
        conn = psycopg2.connect(dbname='postgres', **params)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(f'DROP DATABASE IF EXISTS {DB_NAME}')
        cur.execute(f'CREATE DATABASE {DB_NAME}')
        conn.commit()
        conn.close()

    @classmethod
    def create_tables(cls) -> None:
        '''
        Создает таблицы
        '''
        cls.__create_database()
        with psycopg2.connect(dbname=DB_NAME, **params) as conn:
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

    @classmethod
    def insert_data(cls):
        '''
        Заполняет таблицы
        '''
        data = cls.__read_data()
        with psycopg2.connect(dbname=DB_NAME, **params) as conn:
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
