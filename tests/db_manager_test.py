import psycopg2
import pytest

from database.config import config
from database.db_manager import DBManager

DB_NAME = 'hh_test'
PARAMS = config()
FILE = '../tests/data/test_companies.json'


@pytest.fixture
def dbman():
    return DBManager(db_name=DB_NAME, params=PARAMS, file=FILE)


def test_init(dbman):
    assert dbman._DBManager__file == '../tests/data/test_companies.json'
    assert dbman._DBManager__db_name == 'hh_test'


def test_create_tables(dbman):
    dbman.create_tables()
    with psycopg2.connect(dbname=DB_NAME, **PARAMS) as conn:
        with conn.cursor() as cur:
            cur.execute('''SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'vacancy';''')
            assert cur.fetchone()[0] == 6
            cur.execute('''SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'company';''')
            assert cur.fetchone()[0] == 3


def test_read_data(dbman):
    assert len(dbman._DBManager__read_data()) == 2


def test_insert_data(dbman):
    dbman.insert_data()
    with psycopg2.connect(dbname=DB_NAME, **PARAMS) as conn:
        with conn.cursor() as cur:
            cur.execute('''SELECT COUNT(*) FROM vacancy;''')
            assert cur.fetchone()[0] == 3
            cur.execute('''SELECT COUNT(*) FROM company;''')
            assert cur.fetchone()[0] == 2


def test_get_companies_and_vacancies_count(dbman):
    data = dbman.get_companies_and_vacancies_count()
    assert data == [('ИП Вадим Вереин', 1), ('Metallica', 2)]


def test_get_all_vacancies(dbman):
    data = dbman.get_all_vacancies()
    assert data == [('ИП Вадим Вереин', 'Сваршик', 25000, 'fake_link'), ('Metallica', 'Автор хороших песен', 75000, 'fake_link_2'), ('Metallica', 'Тренер Ларса', None, 'fake_link_3')]


def test_get_avg_salary(dbman):
    data = dbman.get_avg_salary()
    assert data == 50000


def test_get_vacancies_with_higher_salary(dbman):
    data = dbman.get_vacancies_with_higher_salary()
    assert data == [(2, 'Автор хороших песен', 2, 75000, 'LA', 'fake_link_2')]


def test_get_vacancies_with_keyword(dbman):
    data = dbman.get_vacancies_with_keyword('автор')
    assert data == [(2, 'Автор хороших песен', 2, 75000, 'LA', 'fake_link_2')]
    data = dbman.get_vacancies_with_keyword('патологоанатом')
    assert data == []
