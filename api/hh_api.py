from abc import ABC, abstractmethod
from typing import List, Dict
import requests
import logging


class VacancyAPI(ABC):
    """Абстрактный класс для подключения к API платформ с вакансиями"""

    @abstractmethod
    def get_vacancies(self, keyword: str, pages: int, per_page: int) -> List[Dict]:
        """Метод для получения вакансий по ключевому слову"""
        pass


class HeadHunterAPI(VacancyAPI):
    """Класс для работы с API hh.ru"""

    __BASE_URL = "https://api.hh.ru/vacancies"

    def __init__(self, pages: int = 5, per_page: int = 20):
        self.pages = pages
        self.per_page = per_page
        self.vacancies: List[Dict] = []
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO)

    @property
    def base_url(self) -> str:
        return self.__BASE_URL

    def get_vacancies(self, keyword: str, pages: int = None, per_page: int = None) -> List[Dict]:
        """Метод для получения вакансий по ключевому слову"""
        if pages is None:
            pages = self.pages
        if per_page is None:
            per_page = self.per_page

        all_vacancies = []
        for page in range(pages):
            params = {"text": keyword, "page": page, "per_page": per_page}
            response = requests.get(self.__BASE_URL, params=params)

            if response.status_code != 200:
                self.logger.error(f"Ошибка запроса: {response.status_code}")
                continue

            items = response.json().get("items", [])
            all_vacancies.extend(items)

        self.vacancies = all_vacancies
        return all_vacancies

    @staticmethod
    def format_vacancy_data(vacancy: Dict) -> Dict:
        """Форматирование данных вакансий для базы данных"""
        return {
            "title": vacancy.get("name"),
            "salary_from": (vacancy.get("salary") or {}).get("from", 0),  # 0 по умолчанию
            "salary_to": (vacancy.get("salary") or {}).get("to", 0),
            "currency": (vacancy.get("salary") or {}).get("currency", "RUB"),  # RUB по умолчанию,
            "city": vacancy.get("area", {}).get("name"),
            "url": vacancy.get("alternate_url"),
            "employer_name": vacancy.get("employer", {}).get("name"),
            "employer_id": vacancy.get("employer", {}).get("id"),
            "requirements": vacancy.get("snippet", {}).get("requirement"),
            "responsibilities": vacancy.get("snippet", {}).get("responsibility"),
        }

    def fetch_and_format(self, keyword: str) -> List[Dict]:
        """Получение и форматирование данных вакансий"""
        raw_vacancies = self.get_vacancies(keyword)
        return [self.format_vacancy_data(v) for v in raw_vacancies]

    def get_employer_data(self, employer_id: str) -> Dict:
        """Получение данных о компании"""
        employer_url = f"https://api.hh.ru/employers/{employer_id}"
        response = requests.get(employer_url)

        if response.status_code != 200:
            self.logger.error(f"Ошибка запроса к работодателю: {response.status_code}")
            return {}

        return response.json()

    def fetch_all_employers_and_vacancies(self, keyword: str) -> List[Dict]:
        """Получение данных обо всех работодателях и их вакансиях"""
        formatted_vacancies = self.fetch_and_format(keyword)
        employers = {}

        for vacancy in formatted_vacancies:
            employer_id = vacancy["employer_id"]
            if employer_id not in employers:
                employer_data = self.get_employer_data(employer_id)
                employers[employer_id] = employer_data

        # Сборка окончательных данных о вакансиях и работодателях
        for vacancy in formatted_vacancies:
            employer_id = vacancy["employer_id"]
            employer_data = employers.get(employer_id, {})
            vacancy["employer_info"] = employer_data

        return formatted_vacancies


    def get_vacancies_by_employer_id(self, employer_id: int) -> List[Dict]:
        all_vacancies = []

        for page in range(self.pages):
            params = {
                "employer_id": employer_id,
                "page": page,
                "per_page": self.per_page
            }
            response = requests.get(self.__BASE_URL, params=params)

            if response.status_code != 200:
                print(f"Ошибка при получении вакансий: {response.status_code}")
                continue

            items = response.json().get("items", [])
            all_vacancies.extend(items)

        return all_vacancies


# Пример использования:
#api = HeadHunterAPI(pages=2, per_page=5)
#vacancies = api.fetch_all_employers_and_vacancies("Python developer")
#for v in vacancies:
#    print(v)
#hh = HeadHunterAPI(pages=2, per_page=5)
#v = hh.get_vacancies_by_employer_id('1740')
#print(v)

#if None:
#    print('yes')
#else:
#    print('no')