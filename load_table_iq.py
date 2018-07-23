import pyodbc
import os
import sys
from contextlib import closing
from .send_to_sftp import transfer
driver = '{SQL Server}'
server = '192.168.1.1'
user = 'user'
password = 'password'
schema = 'schema'
tablename = 'tablename'
delimiter = '|'
remote_path = 'C:\\Users\\admin\\Downloads\\'
filename = 'test_file.csv'
remote_filepath = os.path.join(remote_path,filename)


class IqLoader:
    """загружает содержимое текстового файла на сервер Sybase IQ"""
    def __init__(self, driver, srv, usr, passw, schema, table, delimiter, filepath):
        self.driver = driver
        self.srv = srv
        self.usr = usr
        self.passw = passw
        self.schema = schema
        self.table = table
        self.delimiter = delimiter
        self.filepath = filepath

    def load_table_iq(self,):
        # функция для загрузки таблицы на Sybase IQ с текстового файла
        try:
            with closing(pyodbc.connect(pyodbc.connect(self.driver, self.srv, self.usr, self.passw))) as conn:
                cursor = conn.cursor()
                cursor.execute('select TOP 1 * from {schema}.{table_name}'.format(schema=self.schema,
                                                                                  table_name=self.table))
                # получаем название полей для последующего создания prepared_stm
                columnname = [field[0] for field in cursor.description]
                fields = ' \'{0}\' null(blanks, zeros),'.format(self.delimiter).join(columnname)
                prepared_stm = """
                    load table {schema}.{table_name}
                            (
                                {fields} 
                                '\x0a' null(blanks, zeros)             
                            )
                            from '{file_path}'
                                quotes off
                                escapes off;
                    """.format(schema=self.schema, table_name=self.table, fields=fields,
                                file_path=self.filepath)
                cursor.execute(prepared_stm)
                conn.commit()
        except Exception:
            print('Load table {0} ERROR:'.format(self.table))
            print(sys.exc_info()[1])


loader = IqLoader(driver=driver, srv=server, usr=user, passw=password, schema=schema, table=tablename,
                  delimiter=delimiter, filepath=remote_filepath)

if __name__ == "__main__":
    transfer.send_to_sftp()
    loader.load_table_iq()
