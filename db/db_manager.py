import psycopg2
from typing import Dict, List
from api.hh_api import HeadHunterAPI  # Импорт API-класса
from config.employers import employer_ids   # Список ID компаний
from dotenv import load_dotenv
import os


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
        INSERT INTO employers (id, name, url, description, open_vacancies)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
        """,
        (
            employer["id"],
            employer["name"],
            employer["url"],
            employer["description"],
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
            vacancy.get("currency"),  # Добавлено поле currency
            vacancy.get("city"),
            vacancy.get("url"),
            vacancy.get("requirements"),
            vacancy.get("responsibilities"),
            vacancy.get("employer_id"),
        )
    )


def load_data():
    hh = HeadHunterAPI(pages=1, per_page=1)
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
            "description": employer_data.get("description", ""),
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


if __name__ == "__main__":
    load_data()



