import psycopg2
import requests
import logging


def get_hh_json(employers: list[str]) -> list[dict]:
    """
    Получает данные: информация о работодателях и их вакансиях. Формат json
    """
    data = []
    for emp in employers:
        url = f'https://api.hh.ru/employers/{emp}'
        company_data = requests.get(url).json()
        vacancy_data = requests.get(company_data['vacancies_url']).json()
        data.append({'employers': company_data, 'vacancies': vacancy_data['items']})
    return data


def create_db(db_name: str, params: dict) -> None:
    """
    Сoздаёт базу данных по вакансиям с сайта hh.ru
    """
    conn = psycopg2.connect(dbname='postgres', **params)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f'DROP DATABASE IF EXISTS {db_name}')
    cur.execute(f'CREATE DATABASE {db_name}')
    conn.close()

    conn = psycopg2.connect(dbname=db_name, **params)
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE employers (
            employer_id SERIAL PRIMARY KEY,
            employer_name VARCHAR UNIQUE,
            url TEXT
            )
        """)

    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE vacancies (
            vacancy_id SERIAL PRIMARY KEY,
            employer_name text REFERENCES employers(employer_name),
            city VARCHAR(50),
            title VARCHAR(200),
            schedule TEXT,
            requirement TEXT,
            responsibility TEXT,
            salary INT,
            url VARCHAR(200),
            FOREIGN KEY(employer_name) REFERENCES employers(employer_name)
            )
        """)
    conn.commit()
    conn.close()
    logging.info("Таблицы 'employers' и 'vacancies' созданы.")


def save_data_to_db(data: list[dict], db_name: str, params: dict) -> None:
    """
    Сохраняет данные о вакансиях в базу данных
    """
    conn = psycopg2.connect(dbname=db_name, **params)
    with conn.cursor() as cur:
        for emp in data:
            cur.execute("""
            INSERT INTO employers (employer_name, url)
            VALUES (%s, %s)
            RETURNING employer_name""",
                        (emp['employers']['name'], emp['employers']['alternate_url'])
                        )
            employer_name = cur.fetchone()[0]
            for vac in emp['vacancies']:
                salary = vac['salary']['from'] if vac['salary'] else None
                cur.execute("""
                INSERT INTO vacancies (employer_name, city, title, schedule, requirement, responsibility,
                salary, url)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                            (employer_name, vac['area']['name'], vac['name'], vac['schedule']['name'],
                             vac['snippet']['requirement'], vac['snippet']['responsibility'], salary,
                             vac['alternate_url'])
                            )
    conn.commit()
    conn.close()
    pass