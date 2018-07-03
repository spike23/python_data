import pyodbc
import os
from contextlib import closing

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


def load_table_iq(driver, server, user, password, schema, tablename, delimiter, remote_filepath):
    # функция для загрузки таблицы на Sybase IQ с текстового файла
    try:
        with closing(pyodbc.connect(pyodbc.connect(driver, server, user, password))) as conn:
            cursor = conn.cursor()
            cursor.execute('select TOP 1 * from {schema}.{table_name}'.format(schema=schema, table_name=tablename))
            # получаем название полей для последующего создания prepared_stm
            columnname = [field[0] for field in cursor.description]
            fields = ' \'{0}\' null(blanks, zeros),'.format(delimiter).join(columnname)
            prepared_stm = """
                load table {schema}.{table_name}
                        (
                            {fields} 
                            '\x0a' null(blanks, zeros)             
                        )
                        from '{file_path}'
                            quotes off
                            escapes off;
                """.format(schema=schema, table_name=tablename, fields=fields,
                            file_path=remote_filepath)
            cursor.execute(prepared_stm)
            conn.commit()
    except BaseException:
        print('Load table {0} ERROR:'.format(tablename))
        raise


if __name__ == "__main__":
    load_table_iq(driver, server, user, password, schema, tablename, delimiter, remote_filepath)
