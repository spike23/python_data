import logging
import pymssql
import sys
from contextlib import closing


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

logging.basicConfig(filename='creator_table_logs.log', level=logging.INFO, format='%(asctime)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')


class CreatorTable:
    """создает таблицу на указанном сервере"""
    def __init__(self, srv, usr, passw, schema, table, fields):
        self.srv = srv
        self.usr = usr
        self.passw = passw
        self.schema = schema
        self.table = table
        self.fields = fields

    def autocreate_table_iq(self,):
        # функция проверяет есть ли таблица на сервере Sybase IQ, если нет - создает
        try:
            with closing(pymssql.connect(self.srv, self.usr, self.passw, tds_version='4.2',
                                          conn_properties='', charset='cp866')) as conn:
                cursor = conn.cursor()
                # формируем поле для create table
                fields_str = ''
                for field in self.fields:
                    fields_str += ''
                    fields_str += field['columnName']
                    fields_str += ' '
                    fields_str += field['columnType']
                    fields_str += ' '
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
                               end""".format(schema=self.schema, tablename=self.table, fields=''.join(fields_str))
                print(sql)
                logging.info(sql)
                cursor.execute(sql)
                conn.commit()
        except Exception:
            print('Creating table {0} ERROR:'.format(self.table))
            print(sys.exc_info()[1])
            logging.info('Creating table {0} ERROR:'.format(self.table))
            logging.info(sys.exc_info()[1])


creator = CreatorTable(srv=server, usr=user, passw=password, schema=schema, table=tablename,
                       fields=fields_dict)

if __name__ == "__main__":
    creator.autocreate_table_iq()
