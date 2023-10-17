import json

import psycopg2
from config import config


def main():
    script_file = 'fill_db.sql'
    create_suppliers = 'create_suppliers.sql'
    json_file = 'suppliers.json'
    db_name = 'my_new_db'

    params = config()
    conn = None

    create_database(db_name, params)
    print(f"БД {db_name} успешно создана")

    params.update({'dbname': db_name})
    try:
        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cur:
                execute_sql_script(cur, script_file)
                print(f"БД {db_name} успешно заполнена")

                execute_sql_script(cur, create_suppliers)
                print("Таблица suppliers успешно создана")

                suppliers = get_suppliers_data(json_file)
                insert_suppliers_data(cur, suppliers)
                print("Данные в suppliers успешно добавлены")

                add_foreign_keys(cur)
                print(f"FOREIGN KEY успешно добавлены")

    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def create_database(db_name, params) -> None:
    """Создает новую базу данных."""
    conn = psycopg2.connect(dbname="postgres", **params)
    cursor = conn.cursor()
    conn.autocommit = True
    try:
        cursor.execute(f"CREATE DATABASE {db_name}")
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        cursor.close()
        conn.close()


def execute_sql_script(cur, script_file) -> None:
    """Выполняет скрипт из файла для заполнения БД данными."""
    with open(script_file, 'r', encoding='UTF-8') as file:
        sql_file = file.read()
        cur.execute(sql_file)


def get_suppliers_data(json_file: str) -> list[dict]:
    """Извлекает данные о поставщиках из JSON-файла и возвращает список словарей с соответствующей информацией."""
    with open(json_file, 'r', encoding='utf-8') as file:
        data = json.load(file)
    return data


def insert_suppliers_data(cur, suppliers: list[dict]) -> None:
    """Добавляет данные из suppliers в таблицу suppliers."""
    for supp in suppliers:
        cur.execute("""
        INSERT INTO suppliers (company_name, contact, address, phone, fax, homepage)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING supplier_id
        """, (supp.get('company_name'),
              supp.get('contact'),
              supp.get('address'),
              supp.get('phone'),
              supp.get('fax'),
              supp.get('homepage')))
        supplier_id = cur.fetchone()[0]

        for product in supp.get('products'):
            cur.execute("""
            UPDATE products
            SET supplier_id = %s
            WHERE product_name = %s
            """, (supplier_id, product))


def add_foreign_keys(cur) -> None:
    """Добавляет foreign key со ссылкой на supplier_id в таблицу products."""
    cur.execute("""
    ALTER TABLE products
    ADD CONSTRAINT fk_products_suppliers
    FOREIGN KEY (supplier_id) REFERENCES suppliers
    """)


if __name__ == '__main__':
    main()
