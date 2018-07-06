import pymssql
import sys
import re
from contextlib import closing

server = '10.50.120.110:5000'
another_server = '10.50.120.120:5000'
user = 'login'
password = 'pass'
mssql_table_from = 'airflow_table'
mssql_table_to = 'table_airflow'
schema = 'schema'


def get_column_types(server, user, password, mssql_table_from):
    """ Получаем данные с сервера """
    try:
        with closing(pymssql.connect(host=server, user=user, password=password, tds_version='4.2',
                                     conn_properties='', charset='cp866')) as connect:
            with closing(connect.cursor(as_dict=True)) as cursor:
                cursor.execute('''select c.name [columnName], t.name [columnType] from sysobjects o
                                  inner join syscolumns c on c.id = o.id
                                  inner join systypes t on t.usertype = c.usertype
                                  where o.type = 'U' and o.name = '%s' 
                              ''' % mssql_table_from
                               )
                cols_types = cursor.fetchall()
                cursor.close()
                connect.commit()
                connect.close()
                return cols_types
    except Exception:
        print("Error while getting column types.")
        print(sys.exc_info()[1])


def get_rows(mssql_table_from, column):
    try:
        with closing(pymssql.connect(host=server, user=user, password=password, tds_version='4.2',
                                     conn_properties='', charset='cp866')) as conn:
            cursor = conn.cursor(as_dict=True)
            columns_list = []
            cols_types = column
            for item in cols_types:
                # Если поле является датой - переконвертируем его
                if re.match('date|time', item['columnType']):
                    columns_list.append(
                        'convert(VARCHAR(25), %s, 112) %s' % (item['columnName'], item['columnName']))
                else:
                    columns_list.append(item['columnName'])

            columns_str = ', '.join(columns_list)

            sql = 'select %s from %s' % (columns_str, mssql_table_from)
            cursor.execute(sql)
            rows = cursor.fetchall()
            cursor.close()
            conn.commit()
            conn.close()
            print('%s', rows)
            print('%s', cols_types)
            return rows
    except Exception:
        print("Error while getting rows.")
        print(sys.exc_info()[1])


def get_data_type(type_name):
    """ Получаем тип данных колонки (для подстановки с оператором ) """
    return {
        'char': 's',
        'varchar': 's',
        'float': 's',
        'int': 'd',
    }.get(type_name, 's')


def bulk_insert_rows(mssql_table_to, rows, cols_types, commit_every=5000):
    # Загрузка данных в таблицу
    try:
        with closing(pymssql.connect(host=another_server, user=user, password=password, tds_version='4.2',
                                     conn_properties='', charset='cp866')) as connect:
            with closing(connect.cursor(as_dict=True)) as cursor:

                values_str_tmp = ''

                columns_list = []

                for item in cols_types:
                    # Сам список полей таблицы
                    columns_list.append(item['columnName'])
                    # Строка подставляемых значений
                    values_str_tmp += '%('
                    values_str_tmp += item['columnName']
                    values_str_tmp += ')'
                    values_str_tmp += get_data_type(item['columnType'])
                    values_str_tmp += ', '

                values_str = values_str_tmp[:len(values_str_tmp) - 2]
                columns_str = ', '.join(columns_list)

                prepared_stm = 'INSERT INTO %s (%s) values (%s)' % (mssql_table_to, columns_str, values_str)
                print('%s', prepared_stm)

                row_count = 0
                # разбивка строк
                row_chunk = []
                for row in rows:
                    row_chunk.append(row)
                    row_count += 1
                    if row_count % commit_every == 0:
                        print('%s', row_chunk)
                        cursor.executemany(prepared_stm, row_chunk)
                        connect.commit()
                        print('[%s] inserted %s rows', mssql_table_to, row_count)
                        row_chunk = []
                cursor.executemany(prepared_stm, row_chunk)
                connect.commit()
                print('%s', row_chunk)
                print('[%s] inserted %s rows', mssql_table_to, row_count)
                cursor.close()
                connect.close()
    except Exception:
        print("Error while inserting.")
        print(sys.exc_info()[1])


if __name__ == "__main__":
    column, cols_types = get_column_types(server, user, password, mssql_table_from)
    rows = get_rows(mssql_table_from, column)
    cols_types = bulk_insert_rows(mssql_table_to, rows, cols_types, commit_every=5000)
