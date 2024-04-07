from config import config
from src.db_manager import DBManager
from src.utils import get_hh_json, create_db, save_data_to_db


def main():
    params = config()  # получение параметров для БД
    # ID компаний работодателей: "Яндекс", "VK", "OZON", "2GIS", "Контур",
    # "Kaspersky", "ЦИАН", "Битрикс24", "NAUMEN", "Skyeng"
    employers = ['1740', '15478', '2180', '64174', '41862',
                 '1057', '1429999', '129044', '42600', '1122462']
    data = get_hh_json(employers)  # получение данных с сайта hh.ru
    create_db('hh_vacancies', params)  # создание БД
    save_data_to_db(data, 'hh_vacancies', params)  # сохранение данных в БД
    db_manager = DBManager('hh_vacancies', params)  # обработка данных для вывода

    # взаимодействие с пользователем
    print('Топ компаний по которым будет осуществляться выборка:\n'
          '"Яндекс", "VK", "OZON", "2GIS", "Контур", "Kaspersky", "Циан",\n'
          '"Битрикс24", "NAUMEN", "Skyeng"\n')
    input('Для продолжения нажмите "Enter"')

    print("Список компаний и количество вакансий:")
    for row in db_manager.get_companies_and_vacancies_count():
        print(f'{row[0]} - {row[1]}')
    input('\nДля продолжения нажмите "Enter"')

    print('Вакансии с указанием названия компании, ЗП, названия вакансии и сслыки на вакансию:')
    for row in db_manager.get_all_vacancies():
        print(f'{row[0]} - {row[1]} - Минимальная заработная плата: {row[2]} - Ссылка: {row[3]}')
    input('\nДля продолжения нажмите "Enter"')

    print('Средняя ЗП по вакансиям:')
    for row in db_manager.get_avg_salary():
        print(f'{row[0]} - {row[1]}')
    input('\nДля продолжения нажмите "Enter"')

    print('Список вакансий у которых ЗП выше средней:')
    for row in db_manager.get_vacancies_with_higher_salary():
        print(f'{row[1]} - {row[3]} - {row[7]} - {row[8]}')
    input('\nДля продолжения нажмите "Enter"')

    keyword = input("Введите ключевое слово для получения вакансий в названии которых оно указано: ")
    for row in db_manager.get_vacancies_with_keyword(keyword):
        print(f'{row[1]} - {row[3]} - {row[7]} - {row[8]}')


if __name__ == '__main__':
    main()