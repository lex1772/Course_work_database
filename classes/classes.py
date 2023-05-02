import psycopg2
import requests
import csv


class HH:
    list_of_vacancy = []
    file = 'vacancy.csv'
    """Класс для работы с платформой HeadHunter"""

    def csv_vacancies(self):
        columns = [i for i in self.list_of_vacancy[0].keys()]
        with open(self.file, 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = columns
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for row in self.list_of_vacancy:
                writer.writerow(row)

    def to_vacancy_list(self, element):
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
        """Функция для получения данных с платформы HeadHunter. Получаем название, зарплату, ссылку, адрес и метро, либо выдаем ошибку, если нет подключения"""
        for i in range(20):
            resp = self.hh_responce(i)
            if resp.status_code == 200:
                vacancies = resp.json()["items"]

                for vacancy in vacancies:
                    element = {"employer": vacancy["employer"]["name"], "vacancy_name": vacancy.get("name"),
                               "salary_from": 0, "salary_to": 0,
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
                            element["salary"]["from"] = 0
                        else:
                            element["salary_from"] = vacancy["salary"]["from"]
                        try:
                            vacancy["salary"]["to"]
                        except TypeError:
                            element["salary"]["to"] = 0
                        else:
                            element["salary_to"] = vacancy["salary"]["to"]
                    else:
                        element["salary_from"] = 0
                        element["salary_to"] = 0

                    try:
                        description = f'{vacancy["snippet"]["requirement"]}. {vacancy["snippet"]["responsibility"]}'
                        element["description"] = description
                    except TypeError:
                        try:
                            element["description"] = vacancy["snippet"]["responsibility"]
                        except TypeError:
                            element["description"] = "Не указано"
                        else:
                            description = f'{vacancy["snippet"]["requirement"].replace("<highlighttext>", "").replace("</highlighttext>", "")}. {vacancy["snippet"]["responsibility"]}'
                            element["description"] = description
                    self.to_vacancy_list(element)

            else:
                print("HH Error:", resp.status_code)

    def clear_vacancies(self):
        """Функция для очистки файла"""
        with open(self.file, "w", encoding="utf-8") as file:
            pass


class DBManager:

    conn = psycopg2.connect(
        host='localhost',
        database='north',
        user='postgres',
        password='12345'
    )

    def load_vacancies(self):
        try:
            with self.conn:
                with self.conn.cursor() as cur:
                    with open(HH.file, "r", encoding="utf-8") as file:
                        rows = csv.reader(file)
                        for row in rows:
                            cur.execute("INSERT INTO vacancies VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", tuple(row))
        finally:
            self.conn.close()

    def get_companies_and_vacancies_count(self):
        try:
            with self.conn:
                with self.conn.cursor() as cur:
                    cur.execute("SELECT DISTINCT employer, COUNT(*) FROM vacancies GROUP BY employer ORDER BY COUNT(*) DESC")
                    rows = cur.fetchall()
                    for row in rows:
                        print(row)
        finally:
            self.conn.close()

    def get_all_vacancies(self):
        try:
            with self.conn:
                with self.conn.cursor() as cur:
                    cur.execute("SELECT employer, vacancy_name, salary_from, salary_to, url FROM vacancies")
                    rows = cur.fetchall()
                    for row in rows:
                        print(row)
        finally:
            self.conn.close()

    def get_avg_salary(self):
        try:
            with self.conn:
                with self.conn.cursor() as cur:
                    cur.execute("SELECT AVG(salary_to) FROM vacancies")
                    rows = cur.fetchall()
                    for row in rows:
                        print(row)
        finally:
            self.conn.close()

    def get_vacancies_with_higher_salary(self):
        try:
            with self.conn:
                with self.conn.cursor() as cur:
                    cur.execute("SELECT employer, vacancy_name, salary_from, salary_to, url FROM vacancies WHERE salary_to > AVG(salary_to)")
                    rows = cur.fetchall()
                    for row in rows:
                        print(row)
        finally:
            self.conn.close()

    def get_vacancies_with_keyword(self, keyword):
        try:
            with self.conn:
                with self.conn.cursor() as cur:
                    cur.execute(f"SELECT employer, vacancy_name, salary_from, salary_to, url FROM vacancies WHERE LOWER(vacancy_name) LIKE %{keyword.lower()}%")
                    rows = cur.fetchall()
                    for row in rows:
                        print(row)
        finally:
            self.conn.close()


