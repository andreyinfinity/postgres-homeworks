"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
import os
from csv import DictReader
from pathlib import Path
from datetime import date

# Пути к файлам из которых заполняем БД
employees_data = Path.joinpath(Path(__file__).parent, "north_data", "employees_data.csv")
customers_data = Path.joinpath(Path(__file__).parent, "north_data", "customers_data.csv")
orders_data = Path.joinpath(Path(__file__).parent, "north_data", "orders_data.csv")


def load_csv(filename: Path) -> list[dict]:
    """Чтение CSV файла и преобразование в список словарей"""
    with open(filename, 'r', newline='', encoding='utf-8') as file:
        csv_file = list(DictReader(file))
    return csv_file


def insert_to_db(query: str, vars_list: list):
    """Подключение к БД north и запись данных в таблицу"""
    conn = psycopg2.connect(
        host='localhost',
        database='north',
        user='postgres',
        password=os.getenv('DB_PASS')
    )
    try:
        with conn:
            with conn.cursor() as cur:
                cur.executemany(query=query, vars_list=vars_list)
    finally:
        conn.close()


def create_employees_query(emp_dict: list[dict]) -> list[tuple]:
    """Создание параметров запроса для таблицы employees"""
    list_emp = []
    for row in emp_dict:
        list_emp.append((int(row["employee_id"]),
                         row["first_name"],
                         row["last_name"],
                         row["title"],
                         date.fromisoformat(row["birth_date"]),
                         row["notes"]))
    return list_emp


def create_customers_query(cust_dict: list[dict]) -> list[tuple]:
    """Создание параметров запроса для таблицы customers"""
    list_cust = []
    for row in cust_dict:
        list_cust.append((row["customer_id"],
                          row["company_name"],
                          row["contact_name"]))
    return list_cust


def create_orders_query(orders_dict: list[dict]) -> list[tuple]:
    """Создание параметров запроса для таблицы orders"""
    list_orders = []
    for row in orders_dict:
        list_orders.append((int(row["order_id"]),
                            row["customer_id"],
                            row["employee_id"],
                            date.fromisoformat(row["order_date"]),
                            row["ship_city"]))
    return list_orders


def main():

    employees = load_csv(employees_data)
    employees_list = create_employees_query(emp_dict=employees)
    insert_to_db(query="INSERT INTO employees VALUES(%s, %s, %s, %s, %s, %s)",
                 vars_list=employees_list)

    customers = load_csv(customers_data)
    customers_list = create_customers_query(cust_dict=customers)
    insert_to_db(query="INSERT INTO customers VALUES(%s, %s, %s)",
                 vars_list=customers_list)

    orders = load_csv(orders_data)
    orders_list = create_orders_query(orders_dict=orders)
    insert_to_db(query="INSERT INTO orders VALUES(%s, %s, %s, %s, %s)",
                 vars_list=orders_list)


if __name__ == "__main__":
    main()
