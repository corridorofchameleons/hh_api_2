from database.db_creator import DBCreator
from database.db_manager import DBManager
from fetch.hh_api import HhApi

companies = {
    'Artel': '3302317',
    'Полиция': '4724218',
    'Правительство Москвы': '895945',
    'Dostavista.ru': '1316038',
    'ЛИДЕР ПЛЮС': '1857985',
    'БУРГЕР КИНГ': '625332',
    'Пятерочка': '1942330',
    'Школа Мужества': '2126364',
    'Европейский Медицинский Центр': '6183',
    'ГБУ Жилищник района Зюзино': '3427780',
}


def main():

    # hh_api = HhApi(companies)
    # hh_api.fetch_data()

    # DBCreator.create_tables()
    # DBCreator.insert_data()

    # vacancies_num_by_company = DBManager.get_companies_and_vacancies_count()
    #
    # all_vacancies = DBManager.get_all_vacancies()
    #
    # avg_salary = DBManager.get_avg_salary()
    #
    # vacancies_with_higher_salary = DBManager.get_vacancies_with_higher_salary()
    #
    # searched_result = DBManager.get_vacancies_with_keyword('сторож')


if __name__ == '__main__':
    main()
