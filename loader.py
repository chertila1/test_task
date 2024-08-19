import os
import pandas as pd
import sqlite3
import numpy as np
from typing import List

folder_path = 'заливка'
UNNAMED = 'Unnamed'

# В pandas не учитывается группировка ячеек, при этом названия столбцов, входящих в сгруппированные, возвращаются как 'Unnamed:'. Потому мы проверяем является ли колонка "безымянной"
def check_column_is_unnamed(column_name: str):
    return not column_name.startswith(UNNAMED)

# Для того, чтобы не учитывать значения в "пустых" колонках, группируем ячейки и возвращаем словарь с количеством сгруппированных ячеек после каждого существующего названия
def get_numbers_of_grouped_cells(column_names: list):
    unnamed_cell = 1

    dict_of_named_columns = {}
    last_cell_name_not_unnamed = ""
    for column_cell in column_names:
        if not column_cell.startswith(UNNAMED):
            dict_of_named_columns[column_cell] = unnamed_cell
            last_cell_name_not_unnamed = column_cell

            unnamed_cell = 1
        else:
            dict_of_named_columns[last_cell_name_not_unnamed] += 1
    return dict_of_named_columns

# Функия для подготовки кортежей к добавлению в базу данных, с учетом сгруппированных в Excel ячеек
def prepare_row(row: tuple, slovar: dict):
    prepared_row: list = []

    indent = 0
    prev_col_name = ""
    for index, column in enumerate(list(slovar.keys())):
        prev_col_name = column
        prepared_row.append(row[indent])
        indent += slovar.get(column)
    return prepared_row

# Функция для подготовки названий столбцов таблицы Excel, для динамического задания названий столбцов таблиц базы данных
def prepare_columns_to_sql_query(columns: List[str]):
    prepared_columns = []
    for column in columns:
        if "." in column:
            prepared_columns.append(column.replace(".", "_"))
        else:
            prepared_columns.append(column)
    return ",".join(prepared_columns)

conn = sqlite3.connect('test.db')

for filename in os.listdir(folder_path):
    if filename.endswith('.xlsx'):  # Если файл является Excel файлом
        file_path = os.path.join(folder_path, filename)
        # Преобразовываем название файла для динамического задания названия таблицы в базе
        transformed_filename = "_".join(filename.split('.')[0].split(" "))

        df = pd.read_excel(file_path, engine='openpyxl', skiprows=7)
        column_names = df.columns.tolist()

        # Чтобы обновлять данные в таблице и избежать дублирования, удаляем таблицу, если она существует
        sql_request_to_delete = f'''DROP TABLE IF EXISTS {transformed_filename};'''
        conn.execute(sql_request_to_delete)
        filtered_columns = list(
            filter(check_column_is_unnamed, column_names))

        # Создаем новую таблицу с соответствующим файлу именем, названия столбцов передаются динамически
        sql_request_to_create = f'''CREATE TABLE {transformed_filename} ({prepare_columns_to_sql_query(filtered_columns)});'''
        conn.execute(sql_request_to_create)

        # Преобразование DataFrame в список кортежей для вставки в базу данных
        data_to_insert = [prepare_row(x, get_numbers_of_grouped_cells(column_names)) for x in df.to_numpy()]

        # Вставка данных в базу данных
        conn.executemany(f"INSERT INTO {transformed_filename} VALUES (?, ?, ?, ?, ?, ?, ?, ?);", data_to_insert)

        print(f'Data from {filename} has been imported.')

conn.commit()
conn.close()

print('All files have been processed.')
