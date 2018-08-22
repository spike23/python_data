import logging
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

logging.basicConfig(filename='mssql_to_mssql_transfer.log', level=logging.INFO, format='%(asctime)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')


class MsSqlToMsSqlTransfer:
    """собирает данные с одной таблицы и отправляет в другую таблицу на разных или на одном сервере"""
    def __init__(self, srv_from, srv_to, usr, passw, table_from, table_to, schema):
        self.srv_from = srv_from
        self.srv_to = srv_to
        self.usr = usr
        self.passw = passw
        self.table_from = table_from
        self.table_to = table_to
        self.schema = schema

    def get_column_types(self,):
        """ Получаем метаданные с сервера """
        try:
            with closing(pymssql.connect(host=self.srv_from, user=self.usr, password=self.passw, tds_version='4.2',
                                         conn_properties='', charset='cp866')) as connect:
                with closing(connect.cursor(as_dict=True)) as cursor:
                    cursor.execute('use {schema}'.format(schema=self.schema))
                    cursor.execute('''select c.name [columnName], t.name [columnType] from sysobjects o
                                    inner join syscolumns c on c.id = o.id
                                    inner join systypes t on t.usertype = c.usertype
                                    where o.type = 'U' and o.name = '{tablename}' 
                                '''.format(tablename=self.table_from)
                                   )
                    cols_types = cursor.fetchall()
                    cursor.close()
                    connect.commit()
                    connect.close()
                    return cols_types
        except Exception:
            print("Error while getting column types.")
            print(sys.exc_info()[1])
            logging.info("Error while getting column types.")
            logging.info(sys.exc_info()[1])

    def get_rows(self, column):
        """ получаем данные с одной таблицы + конвертируем данные, если присутсвует поле 'date' """
        try:
            with closing(pymssql.connect(host=server, user=user, password=password, tds_version='4.2',
                                         conn_properties='', charset='cp866')) as conn:
                cursor = conn.cursor(as_dict=True)
                columns_list = []
                for item in column:
                    # Если поле является датой - переконвертируем его
                    if re.match('date|time', item['columnType']):
                        columns_list.append(
                            'convert(VARCHAR(25), %s, 112) %s' % (item['columnName'], item['columnName']))
                    else:
                        columns_list.append(item['columnName'])

                columns_str = ', '.join(columns_list)

                sql = 'select %s from %s' % (columns_str, self.table_from)
                cursor.execute('use {schema}'.format(schema=self.schema))
                cursor.execute(sql)
                rows = cursor.fetchall()
                cursor.close()
                conn.commit()
                conn.close()
                print('%s' % rows)
                print('%s' % cols_types)
                logging.info('%s' % rows)
                logging.info('%s' % cols_types)
                return rows
        except Exception:
            print("Error while getting rows.")
            print(sys.exc_info()[1])
            logging.info("Error while getting rows.")
            logging.info(sys.exc_info()[1])

    @staticmethod
    def get_data_type(type_name):
        """ Получаем тип данных поля """
        return {
            'char': 's',
            'varchar': 's',
            'float': 's',
            'int': 'd',
        }.get(type_name, 's')

    def bulk_insert_rows(self, rows, cols_types, commit_every=5000):
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
                        values_str_tmp += self.get_data_type(item['columnType'])
                        values_str_tmp += ', '

                    values_str = values_str_tmp[:len(values_str_tmp) - 2]
                    columns_str = ', '.join(columns_list)

                    prepared_stm = 'INSERT INTO %s (%s) values (%s)' % (self.table_to, columns_str, values_str)
                    print('%s' % prepared_stm)
                    logging.info('%s' % prepared_stm)

                    row_count = 0
                    # разбивка строк
                    row_chunk = []
                    for row in rows:
                        row_chunk.append(row)
                        row_count += 1
                        if row_count % commit_every == 0:
                            print('%s' % row_chunk)
                            logging.info('%s' % row_chunk)
                            cursor.execute('use {schema}'.format(schema=self.schema))
                            cursor.executemany(prepared_stm, row_chunk)
                            connect.commit()
                            print('In table {table} inserted {count} rows'.format(table=self.table_to, count=row_count))
                            logging.info('In table {table} inserted {count} rows'.format(table=self.table_to,
                                                                                         count=row_count))
                            row_chunk = []
                    cursor.execute('use test')
                    cursor.executemany(prepared_stm, row_chunk)
                    connect.commit()
                    print('%s' % row_chunk)
                    print('In table {table} inserted {count} rows'.format(table=self.table_to, count=row_count))
                    logging.info('%s' % row_chunk)
                    logging.info('In table {table} inserted {count} rows'.format(table=self.table_to, count=row_count))
                    cursor.close()
                    connect.close()
        except Exception:
            print("Error while inserting.")
            print(sys.exc_info()[1])
            logging.info("Error while inserting.")
            logging.info(sys.exc_info()[1])


transfer_data = MsSqlToMsSqlTransfer(srv_from=server, srv_to=another_server, usr=user, passw=password,
                                     table_from=mssql_table_from, table_to=mssql_table_to, schema=schema )


if __name__ == "__main__":
    cols_types = transfer_data.get_column_types()
    column = transfer_data.get_column_types()
    rows = transfer_data.get_rows(column=column)
    transfer_data.bulk_insert_rows(cols_types=cols_types, rows=rows)
