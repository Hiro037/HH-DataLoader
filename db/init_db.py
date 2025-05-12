import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv
import os

# Загружаем переменные из .env
load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

def create_database():
    """Создает базу данных, если она ещё не существует."""
    conn = psycopg2.connect(
        dbname="postgres",
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    cur = conn.cursor()
    # Создаем новую базу, если ещё нет
    cur.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{DB_NAME}';")
    exists = cur.fetchone()
    if not exists:
        cur.execute(f"CREATE DATABASE {DB_NAME};")
        print(f"База данных '{DB_NAME}' успешно создана!")
    else:
        print(f"База данных '{DB_NAME}' уже существует.")

    cur.close()
    conn.close()

def create_tables():
    """Создает таблицы `employers` и `vacancies` в базе данных."""
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS employers (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            url TEXT NOT NULL,
            open_vacancies INTEGER
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS vacancies (
            id SERIAL PRIMARY KEY,
            title TEXT,
            salary_from INTEGER,
            salary_to INTEGER,
            currency TEXT,
            city TEXT,
            url TEXT,
            requirements TEXT,
            responsibilities TEXT,
            employer_id INTEGER REFERENCES employers(id)
        );
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("Таблицы успешно созданы.")

def refresh_tables():
    """Очищает данные из таблиц `employers` и `vacancies`."""
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cur = conn.cursor()

    cur.execute("""
        TRUNCATE TABLE employers, vacancies RESTART IDENTITY CASCADE;
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("Данные из таблиц удалены.")

def delete_tables():
    """Удаляет таблицы `employers` и `vacancies`."""
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cur = conn.cursor()

    cur.execute("""
            DROP TABLE employers, vacancies CASCADE;
        """)

    conn.commit()
    cur.close()
    conn.close()
    print("Таблицы удалены.")