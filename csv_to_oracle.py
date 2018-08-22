import logging
import cx_Oracle
import os
import csv

local_path = 'C:\\Users\\admin\\Downloads\\'
filename = 'test_file.csv'
local_filepath = os.path.join(local_path, filename)
delimiter = '|'
table = 'airflow_table'

field_types = [{'columnName': 'A', 'columnType': 'VARCHAR2 (10)'},
               {'columnName': 'B', 'columnType': 'VARCHAR2 (10)'},
               {'columnName': 'C', 'columnType': 'number (10)'},
               ]

my_connection = {
    'host': '127.0.0.1',
    'port': '8080',
    'service': 'service_name',
    'user': 'user',
    'password': 'password'
}
conn_str = '{user}/{password}@{host}:{port}/{service}'.format(**my_connection)

logging.basicConfig(filename='csv_to_oracle.log', level=logging.INFO, format='%(asctime)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')


class CsvToOracle:
    """опционально создает/очищает таблицу на сервере и загружает данные с текстового файла на сервер"""
    def __init__(self, conn, table, commit_every, filepath, header, delimiter, fieldtypes):
        self.conn = conn
        self.table = table
        self.commit_every = commit_every
        self.filepath = filepath
        self.header = header
        self.delimiter = delimiter
        self.fieldtypes = fieldtypes

    def bulk_insert_rows(self, ):
        conn = cx_Oracle.connect(self.conn)
        cursor = conn.cursor()
        reader = csv.reader(open(self.filepath), delimiter=self.delimiter)
        # выбор опции: файл с названиями полей или нет
        if self.header:
            target_fields = next(reader)
        else:
            target_fields = [field.get('columnName', 'unknown') for field in self.fieldtypes]
        values = ', '.join(':%s' % i for i in range(1, len(target_fields) + 1))
        prepared_stm = 'insert into {tablename} ({columns}) values ({values})'.format(
            tablename=self.table,
            columns=', '.join(target_fields),
            values=values,
        )
        row_count = 0
        row_chunk = []
        for row in reader:
            row_chunk.append(row)
            row_count += 1
            if row_count % self.commit_every == 0:
                cursor.prepare(prepared_stm)
                cursor.executemany(None, row_chunk)
                conn.commit()
                print('[%s] inserted %s rows', self.table, row_count)
                logging.info('[%s] inserted %s rows', self.table, row_count)
                row_chunk = []
        print(row_chunk)
        logging.info(row_chunk)
        cursor.prepare(prepared_stm)
        cursor.executemany(None, row_chunk)
        conn.commit()
        print('[%s] inserted %s rows', self.table, row_count)
        logging.info('[%s] inserted %s rows', self.table, row_count)
        cursor.close()
        conn.close()

    def auto_create_table(self,):
        fields = []
        for field in field_types:
            fields.append(
                '{field_name} {field_type}'.format(field_name=field['columnName'], field_type=field['columnType']))

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
          '''.format(tablename=self.table, fields=', \n'.join(fields))
        conn = cx_Oracle.connect(self.conn)
        cursor = conn.cursor()
        cursor.execute(query_create)
        cursor.close()
        conn.close()

    def truncate_table(self,):
        query_truncate = 'truncate table {tablename}'.format(tablename=self.table)
        conn = cx_Oracle.connect(self.conn)
        cursor = conn.cursor()
        cursor.execute(query_truncate)
        cursor.close()
        conn.close()


transfer_with_header = CsvToOracle(conn=conn_str, table=table, commit_every=5000, filepath=local_filepath,
                                   header=True, delimiter=delimiter, fieldtypes=field_types)

transfer_without_header = CsvToOracle(conn=conn_str, table=table, commit_every=5000, filepath=local_filepath,
                                      header=False, delimiter=delimiter, fieldtypes=field_types)
if __name__ == '__main__':
    transfer_with_header.auto_create_table()
    transfer_with_header.truncate_table()
    transfer_with_header.bulk_insert_rows()
    transfer_without_header.bulk_insert_rows()
