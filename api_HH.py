import json
import requests

from abc import ABC, abstractmethod


class DataApi(ABC):
    def __init(self):
        pass

    @abstractmethod
    def getting_data_from_api(self):
        pass


class HeadHunterData(DataApi):
    """Класс хранит данные для запроса к API HeadHunter"""
    def __init__(self, url_resource):
        """
        :param url_resource: ссылка на API ресурса
        options: параметры для запроса к API
        """
        self.url_resource = url_resource
        self.options = {
            'page': 1,
            'pages': 50,
            'per_page': 30,
            'text': 'python'
        }

    # Получение данных с сайта HeadHunter
    def getting_data_from_api(self):
        """
        Функция получает список названий работодателей из запроса,
        который возвращает вакансии, найденные по ключевому слову Python
        """
        # Переменная хранит результат запроса
        answer = requests.get(self.url_resource, params=self.options)
        # Декодированный результат
        data = answer.content.decode()
        py_obj = json.loads(data)
        # Список с названием компаний-работодателей
        list_of_employers = [item['employer']['name'] for item in py_obj['items']]
        return list_of_employers

    # Получаем вакансии каждого работодателя
    def getting_data_by_employer_name(self):
        # Получение списка работодателей
        data = self.getting_data_from_api()
        # Список для хранения вакансий от выбранных работодателей
        list_with_all_vacancies = []
        # В цикле бежим по списку с названием работодателей и
        # по каждому делаем запрос на соответствующие вакансий.
        for item in data:
            options = {
                'page': 1,
                'pages': 20,
                'per_page': 10,
                'text': f'{item}'
            }
            # Запрос
            answer = requests.get(self.url_resource, params=options)
            data = answer.content.decode()
            py_obj = json.loads(data)
            # Сортируем пустые запросы и запросы, содержащие результат
            if len(py_obj['items']) == 0:
                continue
            else:
                # Для последующего заполнения данных в PostgreSQL меняем словари,
                # которые содержат None в качестве значения на ноль.
                for element in py_obj['items']:
                    if element['salary'] is None:
                        element['salary'] = {}
                        element['salary'].update([('from', 0), ('to', 0)])
                # Добавляем готовый словарь в список с вакансиями
                list_with_all_vacancies.append(py_obj)
        return list_with_all_vacancies
