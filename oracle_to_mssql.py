import pymssql
import cx_Oracle
import re
from contextlib import closing


my_connection = {
    'host': '127.0.0.2:8080',
    'port': '8080',
    'service': 'service_name',
    'user': 'user',
    'password': 'password'
}

conn_str = '{user}/{password}@{host}:{port}/{service}'.format(**my_connection)
table_ora = 'airflow_table'
mssql_table = 'table_airflow'
mssql_schema = 'schema'
mssql_srv = '127.0.0.1:8080'
mssql_usr = 'user'
mssql_passw = 'password'
commit_every = 5000


class OracleToMsSqlTransfer:
    """отправляет данные с таблицы Oracle в таблицу MsSql"""
    def __init__(self, ora_conn, ora_table, mssql_table, mssql_schema, mssql_srv, mssql_usr, mssql_passw, commit_every):
        self.ora_conn = ora_conn
        self.ora_table = ora_table
        self.mssql_table = mssql_table
        self.mssql_schema = mssql_schema
        self.mssql_srv= mssql_srv
        self.mssql_usr = mssql_usr
        self.mssql_passw = mssql_passw
        self.commit_every = commit_every

    # Получаем тип данных колонки
    @classmethod
    def get_data_type(cls, type_name):
        return {
            'char': 's',
            'varchar': 's',
            'float': 's',
            'int': 'd',
            'date': 's',
        }.get(type_name, 's')

    # стурктура таблицы
    def type_map(self,):
        with closing(pymssql.connect(host=self.mssql_srv, user=self.mssql_usr, password=self.mssql_passw,
                                     tds_version='4.2',  conn_properties='', charset='cp866')) as connect:
            cursor = connect.cursor(as_dict=True)
            cursor.execute('use {schema}'.format(schema=self.mssql_schema))
            query = '''select c.name [columnName], t.name [columnType]
             from sysobjects o
             inner join syscolumns c on c.id = o.id
             inner join systypes t on t.usertype = c.usertype
             where o.type = 'U' and o.name = '{tablename}' '''.format(tablename=self.mssql_table)
            cursor.execute(query)
            result = cursor.fetchall()
            print(result)
            cursor.close()
            connect.commit()
            connect.close()
            return result

    # названия полей для prepared_stm
    def columns(self, ):
        column_lst = [item['columnName'] for item in self.type_map()]
        column_str = ', '.join(column_lst)
        return column_str

    # значения values для prepared_stm
    def values(self,):
        values_str_tmp = ''
        for item in self.type_map():
            # Строка подставляемых значений
            values_str_tmp += '%('
            values_str_tmp += item['columnName']
            values_str_tmp += ')'
            values_str_tmp += self.get_data_type(item['columnType'])
            values_str_tmp += ', '
        values_str = values_str_tmp[:len(values_str_tmp) - 2]
        return values_str

    def bulk_insert_rows(self, column):
        columns_list = []
        for item in column:
            # Если поле является датой - переконвертируем его
            if re.match('date|time', item['columnType']):
                columns_list.append(
                    '''to_char(%s, 'YYYYMMDD') %s ''' % (item['columnName'], item['columnName']))
            else:
                columns_list.append(item['columnName'])

        columns_str = ', '.join(columns_list)
        conn = cx_Oracle.connect(self.ora_conn)
        cursor = conn.cursor()
        query = 'select %s from %s ' % (columns_str, self.ora_table)
        cursor.execute(query)
        columns = [i[0] for i in cursor.description]
        columns_low = [i.lower() for i in columns]
        rows = [dict(zip(columns_low, row)) for row in cursor]

        print(rows)
        print("Transfer Microsoft SQL Server query results to oracle")
        conn_mssql = pymssql.connect(host=self.mssql_srv, user=self.mssql_usr, password=self.mssql_passw,
                                     tds_version='4.2',  conn_properties='', charset='cp866')
        cursor_mssql = conn_mssql.cursor()
        prepared_stm = 'insert into {tablename} ({columns}) values ({values})'.format(tablename=self.mssql_table,
                                                                                      columns=self.columns(),
                                                                                      values=self.values())
        print(prepared_stm)
        cursor_mssql.execute('use {schema}'.format(schema=self.mssql_schema))
        row_count = 0
        row_chunk = []
        for row in rows:
            row_chunk.append(row)
            row_count += 1
            if row_count % self.commit_every == 0:
                cursor.executemany(prepared_stm, row_chunk)
                conn.commit()
                print('{table} inserted {count} rows'.format(table=self.mssql_table, count=row_count))
                row_chunk = []
        cursor_mssql.executemany(prepared_stm, rows)
        conn_mssql.commit()
        print('{table} inserted {count} rows'.format(table=self.mssql_table, count=row_count))
        print("Transfer is successfully finished!")
        cursor_mssql.close()
        conn_mssql.close()


transfer = OracleToMsSqlTransfer(ora_conn=conn_str, ora_table=table_ora, mssql_table=mssql_table,
                                 mssql_schema=mssql_schema, mssql_srv=mssql_srv, mssql_usr=mssql_usr,
                                 mssql_passw= mssql_passw, commit_every=commit_every )

if __name__ == '__main__':
    transfer.bulk_insert_rows(column=transfer.type_map())
