import psycopg2
from typing import Dict, List
from api.hh_api import HeadHunterAPI  # Импорт API-класса
from config.employers import employer_ids   # Список ID компаний
from dotenv import load_dotenv
import os
from db.init_db import refresh_tables


# Загружаем переменные из .env
load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

# 💡 Настройки подключения к БД
DB_CONFIG = {
    "dbname": DB_NAME,
    "user": DB_USER,
    "password": DB_PASSWORD,
    "host": DB_HOST,
    "port": DB_PORT
}

def save_employer(cursor, employer: Dict):
    """Сохраняет работодателя, если его ещё нет"""
    cursor.execute(
        """
        INSERT INTO employers (id, name, url, open_vacancies)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
        """,
        (
            employer["id"],
            employer["name"],
            employer["url"],
            employer["open_vacancies"]
         )
    )


def save_vacancy(cursor, vacancy: Dict):
    """Сохраняет вакансию"""
    cursor.execute(
        """
        INSERT INTO vacancies (title, salary_from, salary_to, currency, city, url, requirements, responsibilities, employer_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
        """,
        (
            vacancy.get("title"),
            vacancy.get("salary_from"),
            vacancy.get("salary_to"),
            vacancy.get("currency"),
            vacancy.get("city"),
            vacancy.get("url"),
            vacancy.get("requirements"),
            vacancy.get("responsibilities"),
            vacancy.get("employer_id"),
        )
    )


def load_data():
    """Загружает данные о работодателях и их вакансиях с hh.ru и сохраняет их в базу данных."""
    refresh_tables()
    hh = HeadHunterAPI()
    connection = psycopg2.connect(**DB_CONFIG)
    cursor = connection.cursor()

    for employer_id in employer_ids:
        print(f"🔍 Обработка работодателя {employer_id}...")
        employer_data = hh.get_employer_data(employer_id)
        vacancies = hh.get_vacancies_by_employer_id(employer_id)

        employer = {
            "id": employer_data["id"],
            "name": employer_data["name"],
            "url": employer_data.get("alternate_url", "N/A"),
            "open_vacancies": employer_data.get("open_vacancies", 0)
        }

        for raw in vacancies:
            #print(raw)
            vacancy = HeadHunterAPI.format_vacancy_data(raw)
            vacancy["employer_id"] = employer["id"]

            #print("Данные работодателя:", employer)
            #print("Данные вакансии:", vacancy)
            #print(employer_data.get("description"))


            save_employer(cursor, employer)
            save_vacancy(cursor, vacancy)

        connection.commit()
        print(f"✅ Загружено {len(vacancies)} вакансий от работодателя {employer_id}")

    cursor.close()
    connection.close()
    print("🎉 Все данные успешно загружены!")


class DBManager:
    """Класс для управления базой данных вакансий и работодателей."""
    def __init__(self):
        """Инициализирует соединение с базой данных."""
        self.conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()

    def get_companies_and_vacancies_count(self):
        """Возвращает список всех компаний и количество вакансий у каждой"""
        self.cursor.execute("""
            SELECT employers.name, employers.open_vacancies
            FROM employers
            LEFT JOIN vacancies ON employers.id = vacancies.employer_id
            GROUP BY employers.name;
        """)
        return self.cursor.fetchall()

    def get_all_vacancies(self):
        """Возвращает список всех вакансий с указанием названия компании, вакансии, зарплаты и ссылки"""
        self.cursor.execute("""
            SELECT employers.name, vacancies.title, vacancies.salary_from, vacancies.salary_to, vacancies.url
            FROM vacancies
            JOIN employers ON vacancies.employer_id = employers.id;
        """)
        return self.cursor.fetchall()

    def get_avg_salary(self):
        """Возвращает среднюю зарплату (используем среднее между salary_from и salary_to)"""
        self.cursor.execute("""
            SELECT AVG((COALESCE(salary_from, 0) + COALESCE(salary_to, 0)) / 2.0)
            FROM vacancies
            WHERE salary_from IS NOT NULL AND salary_to IS NOT NULL;
        """)
        return self.cursor.fetchone()[0]

    def get_vacancies_with_higher_salary(self):
        """Возвращает список вакансий с зарплатой выше средней"""
        avg_salary = self.get_avg_salary()
        self.cursor.execute("""
            SELECT title, salary_from, salary_to, url
            FROM vacancies
            WHERE ((COALESCE(salary_from, 0) + COALESCE(salary_to, 0)) / 2.0) > %s;
        """, (avg_salary,))
        return self.cursor.fetchall()

    def get_vacancies_with_keyword(self, keyword):
        """Возвращает список вакансий, содержащих ключевое слово в названии"""
        self.cursor.execute("""
            SELECT title, salary_from, salary_to, url
            FROM vacancies
            WHERE title ILIKE %s;
        """, (f"%{keyword}%",))
        return self.cursor.fetchall()

    def close(self):
        """Закрывает соединение"""
        self.cursor.close()
        self.conn.close()