import logging
import cx_Oracle
import os
import csv

local_path = 'C:\\Users\\admin\\Downloads\\'
filename = 'test_file.csv'
local_filepath = os.path.join(local_path, filename)
delimiter = '|'
with_header = True
table = 'airflow_table'

field_types = [{'columnName': 'a', 'columnType': 'VARCHAR2 (10)'},
               {'columnName': 'b', 'columnType': 'VARCHAR2 (10)'},
               {'columnName': 'c', 'columnType': 'number (10)'},
               ]

my_connection = {
    'host': '127.0.0.1',
    'port': '8080',
    'service': 'service_name',
    'user': 'usr',
    'password': 'pass'
}

conn_str = '{user}/{password}@{host}:{port}/{service}'.format(**my_connection)
conn = cx_Oracle.connect(conn_str)

reader = csv.reader(open(local_filepath), delimiter=delimiter)
# Если в текстовом файле есть названия полей - выполняется блок next иначе названия выбираются с field_types
column_names_with_header = next(reader)
column_names_without_header = [field.get('columnName', 'unknown') for field in field_types]


class CsvToOracle:
    def __init__(self):
        self.conn = cx_Oracle.connect(conn_str)


    def bulk_insert_rows(self, table, rows, target_fields, commit_every=5000):
        conn_str = '{user}/{password}@{host}:{port}/{service}'.format(**my_connection)
        self.conn = cx_Oracle.connect(conn_str)
        cursor = conn.cursor()
        values = ', '.join(':%s' % i for i in range(1, len(target_fields) + 1))
        prepared_stm = 'insert into {tablename} ({columns}) values ({values})'.format(
            tablename=table,
            columns=', '.join(target_fields),
            values=values,
        )
        row_count = 0
        row_chunk = []
        for row in rows:
            row_chunk.append(row)
            row_count += 1
            if row_count % commit_every == 0:
                cursor.prepare(prepared_stm)
                cursor.executemany(None, row_chunk)
                conn.commit()
                logging.info('[%s] inserted %s rows', table, row_count)
                row_chunk = []
        cursor.prepare(prepared_stm)
        cursor.executemany(None, row_chunk)
        conn.commit()
        logging.info('[%s] inserted %s rows', table, row_count)
        cursor.close()
        conn.close()


fields = []
for field in field_types:
    fields.append('{field_name} {field_type}'.format(field_name=field['columnName'], field_type=field['columnType']))

query_create = '''
                        begin
                        for a in (select 1 from dual 
                                    where not exists (select 1 
                                                        from all_tables 
                                                        where table_name = upper('{tablename}') 
                                                            and owner = sys_context( 'userenv', 'current_schema' ))
                                                            ) loop
                            execute immediate 'create table {tablename}({fields})';
                        end loop;
                        end;
'''.format(tablename=table, fields=', \n'.join(fields))


def auto_create_table():
    conn_str = '{user}/{password}@{host}:{port}/{service}'.format(**my_connection)
    conn = cx_Oracle.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute(query_create)
    cursor.close()
    conn.close()


query_truncate = 'delete from {tablename}'.format(tablename=table)


def truncate_table():
    conn_str = '{user}/{password}@{host}:{port}/{service}'.format(**my_connection)
    conn = cx_Oracle.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute(query_truncate)
    cursor.close()
    conn.close()


transfer = CsvToOracle()

if __name__ == '__main__':
    auto_create_table()
    truncate_table()
    # csv with field description
    transfer.bulk_insert_rows(table=table, rows=reader, target_fields=column_names_with_header)
    # csv without field description
    transfer.bulk_insert_rows(table=table, rows=reader, target_fields=column_names_without_header)
