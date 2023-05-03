#Импортируем классы
from classes.classes import HH, DBManager

def main():
    """Создаем функцию для взаимодействия с пользователем, которая опрашивает какие методы необходимо произвести с таблицей, подгружает данные в таблицу и при завершении работы очищает таблицу"""
    hh = HH()
    sql = DBManager()
    hh.get_request()
    hh.csv_vacancies()
    sql.load_vacancies()
    flag = True
    while flag:
        print("Введите номер метода, который требуется произвести:\n 1) Cписок всех компаний и количество вакансий у каждой компании\n 2) Cписок всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию\n 3) Cредняя зарплата по вакансиям\n 4) Cписок всех вакансий, у которых зарплата выше средней по всем вакансиям\n 5) список всех вакансий, в названии которых содержатся переданные в метод слова")
        try:
            user_num = int(input("Введите номер метода: "))
        except ValueError:
            user_num = int(input("Должно быть число: "))
        if user_num > 5:
            print("Такого метода нет")
        elif user_num == 1:
            sql.get_companies_and_vacancies_count()
        elif user_num == 2:
            sql.get_all_vacancies()
        elif user_num == 3:
            sql.get_avg_salary()
        elif user_num == 4:
            sql.get_vacancies_with_higher_salary()
        elif user_num == 5:
            user_keyword = input("Введите ключевое слово: ")
            sql.get_vacancies_with_keyword(user_keyword)


        user_input= input("Вы хотите продолжить? Наберите 'да' для продолжения или что-угодно для завершения программы: ")
        if user_input.lower() != "да":
            flag = False
            hh.clear_vacancies()
            sql.truncate_table()
            sql.conn_close()
        else:
            flag = True


if __name__ == "__main__":
    """Запускаем функцию"""
    main()
