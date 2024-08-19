import os
import pandas as pd
import sqlite3
from typing import List

folder_path = 'заливка' #Здесь указывается путь к папке, из которой будут читаться файлы
database = 'test.db' #Указываем БД для подключения
unnamed_col_name = 'Unnamed'


# В pandas не учитывается группировка ячеек, при этом названия столбцов, входящих в сгруппированные, возвращаются как 'Unnamed:'. Потому мы проверяем является ли колонка "безымянной"
def is_unnamed(column_name: str) -> bool:
    return not column_name.startswith(unnamed_col_name)


# Для того, чтобы не учитывать значения в "пустых" колонках, группируем ячейки и возвращаем словарь с количеством сгруппированных ячеек после каждого существующего названия
def count_grouped_cells(column_names: list) -> dict:
    unnamed_cell = 1
    dict_of_named_columns = {}
    last_cell_name_not_unnamed = ""

    for column_cell in column_names:
        if not column_cell.startswith(unnamed_col_name):
            dict_of_named_columns[column_cell] = unnamed_cell
            last_cell_name_not_unnamed = column_cell
            unnamed_cell = 1
        else:
            dict_of_named_columns[last_cell_name_not_unnamed] += 1
    return dict_of_named_columns


# Функция для подготовки кортежей к добавлению в базу данных, с учетом сгруппированных в Excel ячеек
def prepare_row(row: tuple, slovar: dict) -> list:
    prepared_row = []
    indent = 0
    prev_col_name = ""

    for index, column in enumerate(list(slovar.keys())):
        prev_col_name = column
        prepared_row.append(row[indent])
        indent += slovar.get(column)
    return prepared_row


# Функция для подготовки названий столбцов таблицы Excel, для динамического задания названий столбцов таблиц базы данных
def prepare_columns_for_sql(columns: List[str]) -> str:
    return ",".join(column.replace(".", "_") for column in columns)


conn = sqlite3.connect(database)

for filename in os.listdir(folder_path):
    if filename.endswith('.xlsx'):
        file_path = os.path.join(folder_path, filename)
        # Преобразовываем название файла для динамического задания названия таблицы в базе
        transformed_filename = "_".join(filename.split('.')[0].split(" "))

        df = pd.read_excel(file_path, engine='openpyxl', skiprows=7)
        column_names = df.columns.tolist()

        # Чтобы обновлять данные в таблице и избежать дублирования, удаляем таблицу, если она существует
        conn.execute(f'''DROP TABLE IF EXISTS {transformed_filename};''')

        filtered_columns = list(filter(is_unnamed, column_names))

        # Создаем новую таблицу с соответствующим файлу именем, названия столбцов передаются динамически
        conn.execute(f'''CREATE TABLE {transformed_filename} ({prepare_columns_for_sql(filtered_columns)});''')

        data_to_insert = [prepare_row(x, count_grouped_cells(column_names)) for x in df.to_numpy()]

        row_values = ', '.join('?' for _ in range(len(data_to_insert[0])))
        conn.executemany(
            f"INSERT INTO {transformed_filename} VALUES ({row_values});",
            data_to_insert
        )

        print(f'Данные из {filename} были успешно импортированы.')

conn.commit()
conn.close()

print('Все файлы были успешно перенесены в БД.')
