import psycopg2

from api_HH import HeadHunterData
from class_DBManager import DBManager

name_host = 'localhost'
name_database = 'vacancies'
name_user = 'postgres'
password = '12345'

user1 = DBManager(name_host, name_database, name_user, password)


def creating_tables():
    """
    Функция создаёт таблицы для хранения данных в PostgreSQL
    :return: None
    """
    # Контекстный менеджер для создания подключения к БД
    with psycopg2.connect(
            host=name_host,
            database=name_database,
            user=name_user,
            password=password
    ) as conn:
        # Контекстный менеджер для создания курсора для работы с данными в БД
        with conn.cursor() as cur:
            # Ловим исключение DuplicateTable, в случае, когда таблицы уже созданы
            try:
                # Формируем команду для создания таблиц
                cur.execute('''CREATE TABLE experience
                (
    id_experience varchar PRIMARY KEY,
    name_experience varchar(256)
                );

                CREATE TABLE employment
                (
    id_employment varchar(256) PRIMARY KEY,
    name_employment varchar(256)
                );

                CREATE TABLE employer
                (
    id_employer varchar(256) PRIMARY KEY,
    name_employer varchar(256)
                );

                CREATE TABLE areas
                (
    id_area varchar(256) PRIMARY KEY,
    name_area varchar(256)
                );

                CREATE TABLE vacancies
                (
    id_vacancy varchar(256) PRIMARY KEY,
    id_experience varchar(256) REFERENCES experience(id_experience),
    id_employment varchar(256) REFERENCES employment(id_employment),
    id_employer varchar(256) REFERENCES employer(id_employer),
    id_area varchar(256) REFERENCES areas(id_area),
    name_vacancy varchar(256),
    salary_from varchar(256) DEFAULT NULL, 
    salary_to varchar(256) DEFAULT NULL, 
    url_vacancy varchar DEFAULT NULL
                );''')
            except psycopg2.errors.lookup('42P07'):
                exit()

        cur.close()
        conn.close()


def get_data_for_database(list_data):
    """
    Функция формирует список, содержащий данные для таблиц отдельных сущностей,
    которые связаны с таблицей "Вакансии".
    :param list_data: Список с вакансиями.
    :return: Список с отфильтрованными данными.
    """
    # Словарь с данными об видах требуемого опыта
    dict_experience = {}
    # Словарь с данными о режимах работ
    dict_employment = {}
    # Словарь с данными об работодателях
    dict_employer = {}
    # Словарь с данными об областях
    dict_area = {}
    # Словарь содержащий все полученный данные.
    dict_values = {}
    # В цикле бежим по вакансиям каждого работодателя
    for item in list_data:
        # Отлавливаем исключение KeyError при отсутствии нужного ключа в предаваемом словаре
        try:
            # Во вложенном цикле получаем информацию для каждой таблицы
            for element in item['items']:
                dict_experience.update([(element['experience']['id'], element['experience']['name'])])
                dict_employment.update([(element['employment']['id'], element['employment']['name'])])
                dict_employer.update([(element['employer']['id'], element['employer']['name'])])
                dict_area.update([(element['area']['id'], element['area']['name'])])
        except KeyError as error:
            print('Not found key in element', error)
    # Присваиваем соответствующие названия для каждого словаря с данными
    dict_values['experience'] = dict_experience
    dict_values['employment'] = dict_employment
    dict_values['employer'] = dict_employer
    dict_values['areas'] = dict_area
    return dict_values


def populating_tables(massive_with_objects):
    """
    Функция заполняет таблицы всех сущностей, кроме "vacancies"
    Данные получены и сгруппированы в функции "get_data_for_database".
    :param massive_with_objects: Данные для внесения в таблицы
    :return: None
    """
    # Устанавливаем соединение с БД
    with psycopg2.connect(
            host=name_host,
            database=name_database,
            user=name_user,
            password=password
    ) as conn:
        # Создаём курсор для работы с данными
        with conn.cursor() as cur:
            for name_table, element in massive_with_objects.items():
                for key, value in element.items():
                    cur.execute(f'INSERT INTO {name_table} VALUES (%s, %s)', (key, value))
                    conn.commit()

    cur.close()
    conn.close()


def populating_table_vacancy(massive_with_data):
    """
    Функция заносит данные о вакансиях таблицу "vacancies"
    :param massive_with_data: Данные для внесения в таблицу.
    :return: None
    """
    # Устанавливаем соединения с БД
    with psycopg2.connect(
            host=name_host,
            database=name_database,
            user=name_user,
            password=password
    ) as conn:
        # Создаём курсор для работы с данными
        with conn.cursor() as cur:
            # В цикле проходим по вакансиям каждого работодателя
            for item in massive_with_data:
                # Во вложенном цикле заносим данные по каждой вакансии
                for element in item['items']:
                    try:
                        cur.execute('INSERT INTO vacancies VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)',
                                    (element['id'], element['experience']['id'], element['employment']['id'],
                                     element['employer']['id'], element['area']['id'], element['name'],
                                     element['salary']['from'], element['salary']['to'], element['url']))
                        conn.commit()
                    # Перехватываем исключение: иногда попадаются вакансии с одинаковыми id
                    except psycopg2.errors.lookup("23504"):
                        continue

        cur.close()
    conn.close()


# Создаём экземпляр класса для получения данных от API HeadHunter
hh_job = HeadHunterData('https://api.hh.ru/vacancies')
# Получаем имена работодателей из вакансий
list_with_vacancy = hh_job.getting_data_by_employer_name()
# Получаем вакансии каждого работодателя
massive_data = get_data_for_database(list_with_vacancy)
# Создаём таблицы в postgreSQL
creating_tables()
# Заполняем таблицы для сущностей в составе каждой вакансии
populating_tables(massive_data)
# Заполняем таблицу со всеми вакансиями
populating_table_vacancy(list_with_vacancy)
