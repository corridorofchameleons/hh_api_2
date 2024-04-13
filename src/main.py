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
    hh_api = HhApi(companies)
    # hh_api.fetch_data()

    db_man = DBManager()
    # db_man.create_tables()
    # db_man.insert_data()

    vacancies_num_by_company = db_man.get_companies_and_vacancies_count()
    all_vacancies = db_man.get_all_vacancies()
    avg_salary = db_man.get_avg_salary()
    vacancies_with_higher_salary = db_man.get_vacancies_with_higher_salary()
    searched_result = db_man.get_vacancies_with_keyword('полицейский')

    print(vacancies_num_by_company)
    print(all_vacancies)
    print(avg_salary)
    print(vacancies_with_higher_salary)
    print(searched_result)


if __name__ == '__main__':
    main()
