from config import config
import psycopg2


class DBManager:
    """
    Класс для получения данных из БД по вакансиям
    """

    def __init__(self, db_name: str, params=config()):
        self.db_name = db_name
        self.params = params

    def get_companies_and_vacancies_count(self) -> list[tuple]:
        """
        Получает список компаний и кол-во вакансий которые они предоставляют
        """
        conn = psycopg2.connect(dbname=self.db_name, **self.params)
        with conn.cursor() as cur:
            cur.execute("""
            SELECT employers.employer_name, COUNT(vacancies.vacancy_id) AS vacancy_count
            FROM employers
            JOIN vacancies USING (employer_name)
            GROUP BY employers.employer_name;""")
            data = cur.fetchall()
        conn.close()
        return data

    def get_all_vacancies(self) -> list[tuple]:
        """
        Получает список всех вакансий с указанием компании, название вакансии, ссылку и ЗП
        """
        conn = psycopg2.connect(dbname=self.db_name, **self.params)
        with conn.cursor() as cur:
            cur.execute("""
            SELECT employers.employer_name, title, salary, vacancies.url
            FROM vacancies
            JOIN employers USING (employer_name);""")
            data = cur.fetchall()
        conn.close()
        return data

    def get_avg_salary(self) -> list[tuple]:
        """
        Получает среднюю ЗП по вакансиям
        """
        conn = psycopg2.connect(dbname=self.db_name, **self.params)
        with conn.cursor() as cur:
            cur.execute("""
            SELECT employers.employer_name, ROUND(AVG(salary))
            FROM vacancies
            JOIN employers USING (employer_name)
            GROUP BY employers.employer_name;""")
            data = cur.fetchall()
        conn.close()
        return data

    def get_vacancies_with_higher_salary(self) -> list[tuple]:
        """
        Получает список вакансий у которых ЗП выше средней
        """
        conn = psycopg2.connect(dbname=self.db_name, **self.params)
        with conn.cursor() as cur:
            cur.execute("""
            SELECT * FROM vacancies
            WHERE salary > (SELECT AVG(salary) FROM vacancies);""")
            data = cur.fetchall()
        conn.close()
        return data

    def get_vacancies_with_keyword(self, keyword) -> list[tuple]:
        """
        Получает список вакансий по ключевому слову
        """
        conn = psycopg2.connect(dbname=self.db_name, **self.params)
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM vacancies "
                        f"WHERE lower(title) LIKE '%{keyword}%' "
                        f"OR lower(title) LIKE '%{keyword}' "
                        f"OR lower(title) LIKE '{keyword}%';")
            data = cur.fetchall()
        conn.close()
        return data