import json
import requests


class HhApi:
    '''
    Класс для получения данных с АПИ
    '''

    BASE_URL = 'https://api.hh.ru/employers/'
    COMPANIES = {
        'Artel': '3302317',
        'Police': '4724218',
    }
    FILE = 'data/temp/companies.json'

    @classmethod
    def __get_data(cls, company_id: str) -> dict:
        '''
        Получает данные компании и ее вакансий по id компании
        '''
        company_data = requests.get(cls.BASE_URL + company_id)
        company = company_data.json()
        vacancies_data = requests.get(company.get('vacancies_url'))
        vacancies = vacancies_data.json().get('items')
        company['vacancies'] = vacancies
        return company

    @classmethod
    def __get_company_list(cls):
        '''
        Собирает список из компаний
        '''
        result = []
        for company_id in cls.COMPANIES.values():
            data = cls.__get_data(company_id)
            result.append(data)

        return result

    @classmethod
    def fetch_data(cls):
        '''
        Записывает данные во временное хранилище
        '''
        data = cls.__get_company_list()
        with open(cls.FILE, 'w') as f:
            json.dump(data, f, ensure_ascii=False)





