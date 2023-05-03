#Импортируем функции
import psycopg2
import requests
import csv


class HH:
    """Класс для работы с API HH с пустым списком вакансий и указанием наименования файла, куда будем загружать данные"""
    list_of_vacancy = []
    file = 'vacancy.csv'

    def csv_vacancies(self):
        """Функция для записи вакансий в файл CSV"""
        columns = [i for i in self.list_of_vacancy[0].keys()]
        with open(self.file, 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = columns
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_NONNUMERIC)
            writer.writeheader()
            for row in self.list_of_vacancy:
                writer.writerow(row)

    def to_vacancy_list(self, element):
        """Функция для добавления вакансий в список"""
        self.list_of_vacancy.append(element)

    def hh_responce(self, page):
        """Функция для отправки запроса на платформу HeadHunter"""
        hh_url = 'https://api.hh.ru/vacancies'
        hh_params = {
            'area': 113,
            'per_page': 100,
            'page': page,
            'employer_id': (
                '2624085', '1740', '2460946', '3529', '15478', '733', '67611', '2324020', '5599249', '895945')
        }
        return requests.get(hh_url, hh_params)

    def get_request(self):
        """Функция для получения данных с платформы HeadHunter. Получаем данные либо выдаем ошибку, если нет подключения"""
        for i in range(20):
            resp = self.hh_responce(i)
            if resp.status_code == 200:
                vacancies = resp.json()["items"]

                for vacancy in vacancies:
                    element = {"employer": vacancy["employer"]["name"], "vacancy_name": vacancy.get("name"),
                               "salary": 0,
                               "url": vacancy["alternate_url"],
                               "experience": vacancy["experience"]["name"]}

                    try:
                        vacancy["address"]["metro"]["station_name"]
                    except TypeError:
                        element["metro"] = "Не указано"
                    else:
                        element["metro"] = vacancy["address"]["metro"]["station_name"]

                    try:
                        vacancy["address"]["raw"]
                    except TypeError:
                        element["address"] = "Не указано"
                    else:
                        element["address"] = vacancy["address"]["raw"]

                    if vacancy["salary"] is not None:
                        try:
                            vacancy["salary"]["from"]
                        except TypeError:
                            element["salary"] = 0
                        except vacancy["salary"]["from"] == "":
                            element["salary"] = 0
                        else:
                            if vacancy["salary"]["from"] is not None:
                                element["salary"] = int(vacancy["salary"]["from"])
                        try:
                            vacancy["salary"]["to"]
                        except TypeError:
                            if element["salary"] != 0:
                                pass
                            else:
                                element["salary"] = 0
                        except vacancy["salary"]["to"] == "":
                            if element["salary"] != 0:
                                pass
                            else:
                                element["salary"] = 0
                        else:
                            if vacancy["salary"]["to"] is not None:
                                element["salary"] = int(vacancy["salary"]["to"])
                    else:
                        element["salary"] = 0

                    self.to_vacancy_list(element)

            else:
                print("HH Error:", resp.status_code)

    def clear_vacancies(self):
        """Функция для очистки файла"""
        with open(self.file, "w", encoding="utf-8") as file:
            pass


class DBManager:
    """Класс для работы с базой данных PostgreeSQL"""
    #Прописываем данные для соединения с базой данных
    conn = psycopg2.connect(
        host='localhost',
        database='course_work_database',
        user='postgres',
        password='12345'
    )

    def load_vacancies(self):
        """Функция для загрузки данных в таблицу"""
        with self.conn:
            with self.conn.cursor() as cur:
                with open(HH.file, "r", encoding="utf-8") as file:
                    file.readline()
                    rows = csv.reader(file)
                    for row in rows:
                        cur.execute(
                            "INSERT INTO vacancies(employer, vacancy_name, salary, url, experience, metro, address) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                            tuple(row))

    def get_companies_and_vacancies_count(self):
        """Функция, которая получает список всех компаний и количество вакансий у каждой компании."""
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute(
                    "SELECT DISTINCT employer, COUNT(*) FROM vacancies GROUP BY employer ORDER BY COUNT(*) DESC")
                rows = cur.fetchall()
                for row in rows:
                    print(row)

    def get_all_vacancies(self):
        """Функция, которая получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию."""
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute("SELECT employer, vacancy_name, salary, url FROM vacancies")
                rows = cur.fetchall()
                for row in rows:
                    print(row)

    def get_avg_salary(self):
        """Функция, которая получает среднюю зарплату по вакансиям."""
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute("SELECT AVG(salary) FROM vacancies")
                rows = cur.fetchall()
                for row in rows:
                    print(row)

    def get_vacancies_with_higher_salary(self):
        """Функция, которая получает список всех вакансий, у которых зарплата выше средней по всем вакансиям."""
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute(
                    "SELECT employer, vacancy_name, salary, url FROM vacancies WHERE salary > (SELECT AVG(salary) FROM vacancies) ORDER BY salary DESC")
                rows = cur.fetchall()
                for row in rows:
                    print(row)

    def get_vacancies_with_keyword(self, keyword):
        """Функция, которая получает список всех вакансий, в названии которых содержатся переданные в метод слова"""
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute(
                    f"SELECT employer, vacancy_name, salary, url FROM vacancies WHERE LOWER(vacancy_name) LIKE '%{keyword.lower()}%'")
                rows = cur.fetchall()
                if len(rows) == 0:
                    print("По вашему запросу нет данных")
                else:
                    for row in rows:
                        print(row)

    def truncate_table(self):
        """Функция, для очистки таблицы"""
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE vacancies")

    def conn_close(self):
        """Функция, для закрытия соединения"""
        self.conn.close()
