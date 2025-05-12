from db.db_manager import DBManager, load_data
from db.init_db import create_database, create_tables


def run():
    """Основная функция для взаимодействия с пользователем, предоставляющая интерфейс для работы с базой данных вакансий.

        Действия:
            - Выводит меню с возможными действиями.
            - Обрабатывает выбор пользователя и выполняет соответствующие запросы к базе данных.
            - Позволяет выход из программы по выбору пользователя.
        """
    create_database()
    create_tables()
    manager = DBManager()
    load_data()

    # Основной цикл взаимодействия с пользователем
    while True:
        # Выводим меню
        print("\nВыберите действие:")
        print("1 - Показать компании и количество вакансий")
        print("2 - Показать все вакансии")
        print("3 - Показать среднюю зарплату")
        print("4 - Показать вакансии с зарплатой выше средней")
        print("5 - Поиск вакансий по ключевому слову")
        print("0 - Выход")

        # Запрашиваем выбор пользователя
        choice = input("Введите номер действия: ")

        if choice == '1':
            # Получаем компании и количество вакансий
            data = manager.get_companies_and_vacancies_count()
            for company, count in data:
                print(f"{company}: {count} открытых вакансий")

        elif choice == '2':
            # Получаем все вакансии
            vacancies = manager.get_all_vacancies()
            for vacancy in vacancies:
                employer_name, title, salary_from, salary_to, url = vacancy
                # Обрабатываем различные случаи указания зарплаты
                if salary_from and salary_to:
                    salary_str = f"от {salary_from} до {salary_to} руб."
                elif salary_from:
                    salary_str = f"от {salary_from} руб."
                elif salary_to:
                    salary_str = f"до {salary_to} руб."
                else:
                    salary_str = "Зарплата не указана"
                print(f"Компания: {employer_name}, Вакансия: {title}, {salary_str}, Ссылка: {url}")

        elif choice == '3':
            # Показываем среднюю зарплату
            avg = manager.get_avg_salary()
            print(f"Средняя зарплата: {avg:.2f} руб.")

        elif choice == '4':
            # Вакансии с зарплатой выше средней
            high_salary_vacancies = manager.get_vacancies_with_higher_salary()
            for vacancy in high_salary_vacancies:
                title, salary_from, salary_to, url = vacancy
                if salary_from and salary_to:
                    salary_str = f"от {salary_from} до {salary_to} руб."
                elif salary_from:
                    salary_str = f"от {salary_from} руб."
                elif salary_to:
                    salary_str = f"до {salary_to} руб."
                else:
                    salary_str = "Зарплата не указана"
                print(f"Вакансия: {title}, {salary_str}, Ссылка: {url}")

        elif choice == '5':
            # Поиск вакансий по ключевому слову
            keyword = input("Введите ключевое слово: ")
            filtered = manager.get_vacancies_with_keyword(keyword)
            for vacancy in filtered:
                title, salary_from, salary_to, url = vacancy
                if salary_from and salary_to:
                    salary_str = f"от {salary_from} до {salary_to} руб."
                elif salary_from:
                    salary_str = f"от {salary_from} руб."
                elif salary_to:
                    salary_str = f"до {salary_to} руб."
                else:
                    salary_str = "Зарплата не указана"
                print(f"Вакансия: {title}, {salary_str}, Ссылка: {url}")

        elif choice == '0':
            # Выход из программы
            print("Выход из программы.")
            break

        else:
            # Обработка неверного ввода
            print("Неверный ввод. Попробуйте снова.")

    # Закрываем соединение с базой данных
    manager.close()
