import json
import requests

BASE_URL = 'https://api.hh.ru/employers/'
FILE = '../data/temp/companies.json'


class HhApi:
    '''
    Класс для получения данных с АПИ
    '''

    def __init__(self, companies, file=FILE, url=BASE_URL):
        self.__companies = companies
        self.__file = file
        self.__base_url = url
        self.__fetched = 0

    def __get_data(self, company_id: str) -> dict:
        '''
        Получает данные компании и ее вакансий по id компании
        '''

        company_data = requests.get(self.__base_url + company_id)
        company = company_data.json()
        if company.get('id'):  # если компания существует
            vacancies_data = requests.get(company.get('vacancies_url'))
            vacancies = vacancies_data.json().get('items')
            company['vacancies'] = vacancies
            self.__fetched += 1

        return company

    def __get_company_list(self) -> list[dict]:
        '''
        Собирает список из компаний
        '''
        result = []
        for company_id in self.__companies.values():
            data = self.__get_data(company_id)
            result.append(data)

        return result

    def fetch_data(self) -> None:
        '''
        Записывает данные во временное хранилище
        '''
        data = self.__get_company_list()
        print(f'Получены данные по {self.__fetched} компании/ям из {len(self.__companies)}')

        with open(self.__file, 'w') as f:
            json.dump(data, f, ensure_ascii=False)
