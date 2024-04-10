import json
import psycopg2

from db_config import config


class DBCreator:
    DB_NAME = 'hh'
    params = config()
    FILE = '../data/temp/companies.json'

    @classmethod
    def __read_data(cls):
        with open(cls.FILE) as f:
            data = json.load(f)
        return data

    @classmethod
    def __create_database(cls) -> None:
        conn = psycopg2.connect(dbname='postgres', **cls.params)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(f'DROP DATABASE IF EXISTS {cls.DB_NAME}')
        cur.execute(f'CREATE DATABASE {cls.DB_NAME}')
        conn.commit()
        conn.close()

    @classmethod
    def create_tables(cls) -> None:
        cls.__create_database()
        with psycopg2.connect(dbname=cls.DB_NAME, **cls.params) as conn:
            with conn.cursor() as cur:
                cur.execute('''
                DROP TABLE IF EXISTS company;
                
                CREATE TABLE company (
                    company_id INT PRIMARY KEY,
                    company_name VARCHAR(50),
                    company_link VARCHAR(50)
                );
                ''')

                cur.execute('''
                DROP TABLE IF EXISTS vacancy;
                
                CREATE TABLE vacancy (
                    vacancy_id SERIAL PRIMARY KEY,
                    vacancy_title VARCHAR(100),
                    company_id INT,
                    vacancy_salary INT,
                    
                    CONSTRAINT fk_company FOREIGN KEY (company_id) REFERENCES company(company_id)
                    ON DELETE CASCADE
                );
                ''')

    @classmethod
    def insert_data(cls):
        data = cls.__read_data()
        with psycopg2.connect(dbname=cls.DB_NAME, **cls.params) as conn:
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
