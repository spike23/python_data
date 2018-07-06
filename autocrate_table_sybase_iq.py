import pyodbc
import sys
from contextlib import closing

driver = '{SQL Server}'
server = '192.168.1.1'
user = 'user'
password = 'password'
schema = 'schema'
tablename = 'tablename'
fields_dict = [{'columnName': 'field_1', 'columnType': 'VARCHAR', 'columnLength': '(25)', 'constraint': 'NOT NULL'},
               {'columnName': 'field_2', 'columnType': 'VARCHAR', 'columnLength': '(25)', 'constraint': 'NOT NULL'},
               {'columnName': 'field_3', 'columnType': 'DATE', 'constraint': 'NOT NULL'},
               {'columnName': 'field_4', 'columnType': 'VARCHAR', 'columnLength': '(5)', 'constraint': 'NOT NULL'},
               {'columnName': 'field_5', 'columnType': 'NUMERIC', 'columnLength': '(15, 2)', 'constraint': 'NOT NULL'},
               {'columnName': 'field_6', 'columnType': 'NUMERIC', 'columnLength': '(15, 2)', 'constraint': 'NULL'},
               {'columnName': 'field_7', 'columnType': 'DATE', 'constraint': 'NULL'},
               {'columnName': 'field_8', 'columnType': 'NUMERIC', 'columnLength': '(15, 2)', 'constraint': 'NULL'}]


def autocreate_table_iq(driver, server, user, password, schema, tablename, fields_dict):
    # функция проверяет есть ли таблица на сервере Sybase IQ, если нет - создает
    try:
        with closing(pyodbc.connect(driver, server, user, password)) as conn:
            cursor = conn.cursor()


            # формируем поле для create table
            fields_str = ''
            for field in fields_dict:
                fields_str += ''
                fields_str += field['columnName']
                fields_str += ' '
                fields_str += field['columnType']
                fields_str += ' '
                if 'columnLength' in field:
                    fields_str += field['columnLength']
                fields_str += ' '
                fields_str += field['constraint']
                fields_str += ',\n'
                if 'columnLength' in field:
                    fields_str += '({0})'.format(str(field['columnLength'])) \
                        if isinstance(field['columnLength'], int) else field['columnLength']
                if 'constraint' in field:
                    fields_str += ' '
                    fields_str += field['constraint']
                else:
                    fields_str += ' null'
                fields_str += ',\n'

            sql = """IF object_id('{schema}.{tablename}') is null
                           begin
                              execute 
                              (
                                  'CREATE TABLE {schema}.{tablename}
                                   ( {fields}
                                   )'
                               )
                           end""".format(schema=schema, tablename=tablename, fields=''.join(fields_str))

            cursor.execute(sql)
            conn.commit()
    except Exception:
        print('Creating table {0} ERROR:'.format(tablename))
        print(sys.exc_info()[1])
        


if __name__ == "__main__":
    autocreate_table_iq(driver, server, user, password, schema, tablename, fields_dict)