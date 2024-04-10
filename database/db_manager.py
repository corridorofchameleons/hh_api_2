import psycopg2

from database.db_config import config
from database.db_creator import DB_NAME

params = config() | {'dbname': DB_NAME}


class DBManager:
    @staticmethod
    def get_companies_and_vacancies_count() -> list[tuple]:
        '''
        получает список всех компаний и количество вакансий у каждой компании
        '''

        with psycopg2.connect(**params) as conn:
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

    @staticmethod
    def get_all_vacancies() -> list[tuple]:
        '''
        получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию
        '''

        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cur:
                cur.execute('''
                SELECT company_name, vacancy_title, salary, vacancy_link
                FROM vacancy
                JOIN company 
                USING (company_id);
                ''')

                data = cur.fetchall()

        return data

    @staticmethod
    def get_avg_salary() -> int:
        '''
        получает среднюю зарплату по вакансиям
        '''
        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cur:
                cur.execute('''
                SELECT AVG(salary)::DECIMAL(10, 2)
                FROM vacancy;
                ''')

                data = cur.fetchone()[0]

        return data

    @staticmethod
    def get_vacancies_with_higher_salary() -> list[tuple]:
        '''
        получает список всех вакансий, у которых зарплата выше средней по всем вакансиям
        '''
        with psycopg2.connect(**params) as conn:
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

    @staticmethod
    def get_vacancies_with_keyword(keyword: str) -> list[tuple]:
        '''
        получает список всех вакансий, в названии которых содержатся переданные в метод слова
        '''
        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cur:
                cur.execute(f'''
                SELECT *
                FROM vacancy
                WHERE LOWER(vacancy_title) LIKE '%{keyword.lower()}%';
                ''')

                data = cur.fetchall()

        return data
