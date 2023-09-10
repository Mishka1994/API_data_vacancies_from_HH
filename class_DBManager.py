import psycopg2


class DBManager:
    """
    Класс устанавливает соединения с Базой Данных и
    предоставляет функции для получения данных из БД
    """
    def __init__(self, host, database, user, password):
        """
        :param host: Имя хоста
        :param database: Название Базы Данных
        :param user: Имя пользователя
        :param password: пароль
        """
        self.host = host
        self.database = database
        self.user = user
        self.__password = password
        self.conn = psycopg2.connect(host=self.host,
                                     database=self.database,
                                     user=self.user,
                                     password=self.__password
                                     )
        self.cur = self.conn.cursor()

    def __repr__(self):
        return f'{self.__class__.__name__},\n({self.host}, {self.database}, {self.user}', \
               f'{self.conn}, {self.cur})'

    def __str__(self):
        return f'Имя класса: {self.__class__.__name__}\n' \
               f'Имя хоста: {self.host}, название БД: {self.database}\n' \
               f'Имя пользователя БД: {self.user} ' \
               f'Соединение с БД: {self.conn},\nКурсор для выполнения операция с БД: {self.cur}'

    def __del__(self):
        self.cur.close()
        self.conn.close()

    def get_companies_and_vacancies_count(self):
        """
        Функция получает список всех компаний и количество вакансий у каждой компании.
        :return: Возвращает список кортежей с результатом.
        """
        self.cur.execute('''SELECT name_employer, COUNT(*)
        FROM vacancies
        INNER JOIN employer ON vacancies.id_employer=employer.id_employer
        GROUP BY name_employer''')
        result = self.cur.fetchall()
        return result

    def get_all_vacancies(self):
        """
        Функция получает список всех вакансий с указанием названия компании,
        название вакансии и зарплаты, и ссылки на вакансию.
        :return: Возвращает список кортежей с результатом.
        """
        self.cur.execute('''SELECT name_employer, name_vacancy, salary_from, salary_to, url_vacancy
        FROM vacancies
        INNER JOIN employer ON vacancies.id_employer=employer.id_employer''')
        result = self.cur.fetchall()
        return result

    def get_avg_salary(self):
        """
        Функция получает среднюю зарплату по вакансиям.
        :return: Возвращает список кортежей с результатом.
        """
        self.cur.execute('''SELECT AVG(salary_from::integer + salary_to::integer)
        FROM vacancies
        WHERE salary_from::integer>=0 AND salary_to::integer>=0''')
        result = self.cur.fetchall()
        return result

    def get_vacancies_with_higher_salary(self):
        """
        Функция получает список всех вакансий, у которых зарплата выше средней по всем вакансиям.
        :return: Возвращает список кортежей с результатом.
        """
        self.cur.execute('''SELECT * 
        FROM vacancies
        WHERE 
        salary_from::integer > (SELECT AVG(salary_from::integer + salary_to::integer)
        FROM vacancies
        WHERE salary_from::integer>=0 AND salary_to::integer>=0)
        OR
        salary_to::integer > (SELECT AVG(salary_from::integer + salary_to::integer)
        FROM vacancies
        WHERE salary_from::integer>=0 AND salary_to::integer>=0)''')
        result = self.cur.fetchall()
        return result

    def get_vacancies_with_keyword(self, text):
        """
        Функция получает список всех вакансий, в названии которых содержатся переданные в метод слова, например python.
        :param text: Содержит слово для поиска.
        :return: Возвращает список кортежей с результатом.
        """
        self.cur.execute(f"""SELECT * 
        FROM vacancies
        WHERE name_vacancy LIKE '%{text}%'""")
        result = self.cur.fetchall()
        return result
